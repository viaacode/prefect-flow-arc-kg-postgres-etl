from enum import Enum

from functools import partial
from pendulum import DateTime
from prefect import task, flow, get_run_logger
from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import StateType
from prefect.runtime import deployment, flow_run
from prefect.runtime.flow_run import get_scheduled_start_time
from prefect._internal.concurrency.api import create_call, from_sync
from prefect_meemoo.prefect.deployment import (
    DeploymentModel,
    change_deployment_parameters,
    run_deployment_task,
    task_failure_hook_change_deployment_parameters,
    get_deployment_parameter,
    check_deployment_blocking,
    check_deployment_running_flows
)

@flow(name="prefect_flow_arc")
def main_flow(
    deployment_kg_view_flow: DeploymentModel,
    deployment_arc_db_load_flow: DeploymentModel,
    deployment_arc_indexer_flow: DeploymentModel,
    last_modified: DateTime = None,
    or_ids: list[str] = None,
    full_sync: bool = False,
):
    """
    Flow to run all ETL flows for the knowledge graph.
    """
    logger = get_run_logger()
    # Ensure the deployment is ready to run
    if check_deployment_running_flows(
        name=f"{flow_run.get_flow_name()}/{deployment.get_name()}",
    ):
        logger.info("Deployment is already running, skipping execution.")
        return
    if check_deployment_blocking(
        [
            deployment_kg_view_flow,
            deployment_arc_db_load_flow,
            deployment_arc_indexer_flow,
        ]
    ):
        logger.info("Blocking deployments are running, skipping execution.")
        return
        
    current_start_time = get_scheduled_start_time().in_timezone("Europe/Brussels")
    logger.info(f"Current start time: {current_start_time}")

    kg_view_parameter_change = change_deployment_parameters(
        name = deployment_kg_view_flow.name,
        parameters={
            "last_modified": last_modified,
            "or_ids": or_ids,
            "full_sync": full_sync or deployment_kg_view_flow.full_sync,
        }
    ) if deployment_kg_view_flow.active else None

    kg_view_flow = run_deployment_task.submit(
        name=deployment_kg_view_flow.name,
        wait_for=[kg_view_parameter_change]
    ) if deployment_kg_view_flow.active else None

    arc_db_load_parameter_change = change_deployment_parameters.submit(
        name=deployment_arc_db_load_flow.name,
        parameters={
            "last_modified": last_modified,
            "or_ids": or_ids,
            "full_sync": full_sync or deployment_kg_view_flow.full_sync
        },
        wait_for=[kg_view_flow]
    ) if deployment_arc_db_load_flow.active else None

    arc_db_load_result = run_deployment_task.submit(
        name=deployment_arc_db_load_flow.name, wait_for=[kg_view_flow, arc_db_load_parameter_change]
    ) if deployment_arc_db_load_flow.active else None

    arc_indexer_result = run_deployment_task.submit(
        name=deployment_arc_indexer_flow.name, wait_for=[arc_db_load_result]
    ) if deployment_arc_indexer_flow.active else None

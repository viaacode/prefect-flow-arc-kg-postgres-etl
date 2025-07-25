
from pendulum import DateTime
from prefect import flow, get_run_logger
from prefect.runtime import deployment, flow_run
from prefect.runtime.flow_run import get_scheduled_start_time
from prefect_meemoo.prefect.deployment import (
    DeploymentModel,
    change_deployment_parameters,
    run_deployment_task,
    check_deployment_blocking,
    check_deployment_running_flows,
    check_deployment_last_flow_run_failed
)

@flow(name="prefect_flow_arc")
def main_flow(
    deployment_kg_view_flow: DeploymentModel,
    deployment_arc_db_load_flow: DeploymentModel,
    deployment_arc_alto_to_json_flow: DeploymentModel,
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
        # Max running = 1, because this one is counted as well
        max_running=1
    ):
        logger.warning("Deployment is already running, skipping execution.")
        return
    # Deployments are never blocking if they are in full sync mode
    blocking_deployments = [dep for dep in [deployment_kg_view_flow, deployment_arc_db_load_flow, deployment_arc_indexer_flow, deployment_arc_alto_to_json_flow, deployment_arc_indexer_flow] if not dep.full_sync]
    if check_deployment_blocking(blocking_deployments)  and not full_sync:
        logger.warning("Blocking deployments are running, skipping execution.")
        return
        
    current_start_time = get_scheduled_start_time().in_timezone("Europe/Brussels")
    logger.info(f"Current start time: {current_start_time}")

    # Check if the last flow run of the knowledge graph view flow failed
    kg_view_last_flow_run_failed = check_deployment_last_flow_run_failed(deployment_kg_view_flow.name)
    # Check if the last flow run of the knowledge graph to postgres flow failed
    kg_to_postgres_last_flow_run_failed = check_deployment_last_flow_run_failed(deployment_arc_db_load_flow.name)
    # Check if the last flow run of the arc alto to json flow failed
    arc_alto_to_json_last_flow_run_failed = check_deployment_last_flow_run_failed(deployment_arc_alto_to_json_flow.name)
    # Check if the last flow run of the arc indexer flow failed
    arc_indexer_last_flow_run_failed = check_deployment_last_flow_run_failed(deployment_arc_indexer_flow.name)

    # Only change the full_sync parameter if the flow is active
    kg_view_parameter_change = change_deployment_parameters.submit(
        name = deployment_kg_view_flow.name,
        parameters={
            # Change the full_sync parameter based on the input of the main flow or the deploymentmodel's full_sync parameter
            "full_sync": full_sync or deployment_kg_view_flow.full_sync,
        }
    ) if deployment_kg_view_flow.active else None

    # Only change the last_modified parameter if the flow is active and the last flow run did not fail or if it is a full sync
    kg_view_last_modified_parameter_change = change_deployment_parameters.submit(
        name=deployment_kg_view_flow.name,
        parameters={
            "last_modified": last_modified,
        },
        wait_for=[kg_view_parameter_change]
    ) if deployment_kg_view_flow.active and (
        (not kg_view_last_flow_run_failed)
        or full_sync or deployment_kg_view_flow.full_sync
    ) else None

    # Run the knowledge graph view flow if it is active
    kg_view_flow = run_deployment_task.submit(
        name=deployment_kg_view_flow.name,
        wait_for=[kg_view_parameter_change, kg_view_last_modified_parameter_change]
    ) if deployment_kg_view_flow.active else None

    # Only change the full_sync parameter if the flow is active
    kg_to_postgres_parameter_change = change_deployment_parameters.submit(
        name=deployment_arc_db_load_flow.name,
        parameters={
            # Change the full_sync parameter based on the input of the main flow or the deploymentmodel's full_sync parameter
            "full_sync": full_sync or deployment_kg_view_flow.full_sync
        },
        wait_for=[kg_view_flow]
    ) if deployment_arc_db_load_flow.active else None

    # Only change the last_modified parameter if the flow is active and the last flow run or its dependant flow runs did not fail or if it is a full sync
    kg_to_postgres_last_modified_parameter_change = change_deployment_parameters.submit(
        name=deployment_arc_db_load_flow.name,
        parameters={
            "last_modified": last_modified,
        },
        wait_for=[kg_to_postgres_parameter_change]
    ) if deployment_arc_db_load_flow.active and (
        not any([kg_view_last_flow_run_failed, kg_to_postgres_last_flow_run_failed])
        or full_sync or deployment_arc_db_load_flow.full_sync
    ) else None

    # Run the knowledge graph to postgres flow if it is active
    kg_to_postgres_result = run_deployment_task.submit(
        name=deployment_arc_db_load_flow.name, wait_for=[kg_view_flow, kg_to_postgres_parameter_change, kg_to_postgres_last_modified_parameter_change]
    ) if deployment_arc_db_load_flow.active else None

    # Only change the full_sync parameter if the flow is active
    arc_alto_to_json_parameter_change = change_deployment_parameters.submit(
        name=deployment_arc_alto_to_json_flow.name,
        parameters={
            # Change the full_sync parameter based on the input of the main flow or the deploymentmodel's full_sync parameter
            "full_sync": full_sync or deployment_arc_alto_to_json_flow.full_sync,
        },
        wait_for=[kg_to_postgres_result]
    ) if deployment_arc_alto_to_json_flow.active else None

    # Only change the last_modified parameter if the flow is active and the last flow run or its dependant flow runs did not fail or if it is a full sync
    arc_alto_to_json_last_modified_parameter_change = change_deployment_parameters.submit(
        name=deployment_arc_alto_to_json_flow.name,
        parameters={
            "last_modified": last_modified,
        },
        wait_for=[arc_alto_to_json_parameter_change]
    ) if deployment_arc_alto_to_json_flow.active and (
        not any([
            arc_alto_to_json_last_flow_run_failed,
            kg_view_last_flow_run_failed,
            kg_to_postgres_last_flow_run_failed
        ])
        or full_sync or deployment_arc_alto_to_json_flow.full_sync
    ) else None

    # Run the arc alto to json flow if it is active
    arc_alto_to_json_result = run_deployment_task.submit(
        name=deployment_arc_alto_to_json_flow.name, wait_for=[kg_to_postgres_result, arc_alto_to_json_parameter_change, arc_alto_to_json_last_modified_parameter_change]
    ) if deployment_arc_alto_to_json_flow.active else None

    # Only change the full_sync parameter and the or_ids filter if the flow is active
    arc_indexer_parameter_change = change_deployment_parameters.submit(
        name=deployment_arc_indexer_flow.name,
        parameters={
            "or_ids": or_ids,
            "full_sync": full_sync or deployment_arc_indexer_flow.full_sync,
        },
        wait_for=[arc_alto_to_json_result]

    ) if deployment_arc_indexer_flow.active else None

    # Only change the last_modified parameter if the flow is active and the last flow run or its dependant flow runs did not fail or if it is a full sync
    arc_indexer_last_modified_parameter_change = change_deployment_parameters.submit(
        name=deployment_arc_indexer_flow.name,
        parameters={
            "last_modified": last_modified,
        },
        wait_for=[arc_indexer_parameter_change]
    ) if deployment_arc_indexer_flow.active and (
        not any([
            arc_indexer_last_flow_run_failed,
            arc_alto_to_json_last_flow_run_failed,
            kg_view_last_flow_run_failed,
            kg_to_postgres_last_flow_run_failed
        ])
        or full_sync or deployment_arc_indexer_flow.full_sync
    ) else None

    # Run the arc indexer flow if it is active
    run_deployment_task.submit(
        name=deployment_arc_indexer_flow.name, wait_for=[kg_to_postgres_result, arc_alto_to_json_result, arc_indexer_parameter_change, arc_indexer_last_modified_parameter_change]
    ) if deployment_arc_indexer_flow.active else None

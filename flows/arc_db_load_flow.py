import os
import time

from pendulum.datetime import DateTime
from prefect import flow, get_run_logger, task
from prefect.states import Completed
from prefect.task_runners import ConcurrentTaskRunner
from prefect.blocks.system import Secret
from prefect_meemoo.config.last_run import save_last_run_config
from prefect_meemoo.triplydb.tasks import run_javascript
from prefect_sqlalchemy.credentials import DatabaseCredentials
from flows.arc_db_load_index_tables_flow import get_min_date

@task
def wait_until_hour(hour: int):
    now = DateTime.now('Europe/Brussels')
    logger = get_run_logger()
    logger.info(f"Current time is {now}. Waiting until {hour}:00 to start...")
    while now.hour != hour:
        time.sleep(300)  # Sleep for 5 minutes
        now = DateTime.now('Europe/Brussels')
    return Completed()

@flow(
    name="prefect_flow_arc_db_load",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def arc_db_load_flow(
    endpoint_block: str = "sparql-endpoint",
    base_path: str = "/opt/prefect/typescript/",
    script_path: str = "lib/",
    db_block_name: str = "local",
    db_ssl: bool = True,
    db_pool_min: int = 0,
    db_pool_max: int = 5,
    db_loading_batch_size: int = 100,
    record_limit: int = None,
    last_modified: DateTime = None,
    or_ids: list[str] = None,
    full_sync: bool = False,
    sync_tables: list[str] = None,
    full_sync_hour: int = 0,
    debug_mode: bool = False,
    logging_level: str = os.environ.get("PREFECT_LOGGING_LEVEL"),
):
    """Flow to query the TriplyDB dataset and update the graphql database.
    Blocks:
        - triplydb (TriplyDBCredentials): Credentials to connect to MediaHaven
        - hetarchief-tst (PostgresCredentials): Credentials to connect to the postgres database
    """
    # Load credentials
    endpoint = Secret.load(endpoint_block)
    postgres_creds = DatabaseCredentials.load(db_block_name)

    # Run javascript which loads graph into postgres
    load_db_script: str = "2_database_load.js"
    logger = get_run_logger()
    if or_ids:
        logger.warning("'or_ids' were provided, but are expected to also be passed to prefect_flow_arc_kg_view first. The full result of that flow will be used.")


    if full_sync:
        logger.info(f"Full sync requested, waiting until {full_sync_hour}:00 to start...")
        wait_period = wait_until_hour.submit(hour=full_sync_hour).result()
        logger.info("Starting full sync.")
        last_modified = None
        if or_ids:
            last_modified = get_min_date()
            logger.info(f"'or_ids' were provided, setting 'last_modified' to {last_modified}. Tables will not be truncated.")

    loading = run_javascript.with_options(
            name=f"Sync KG to services with {load_db_script}",
        ).submit(
            script_path=base_path + script_path + load_db_script,
            base_path=base_path,
            postgres=postgres_creds,
            record_limit=record_limit,
            batch_size=db_loading_batch_size,
            since=last_modified,
            debug_mode=debug_mode,
            logging_level=logging_level,
            postgres_ssl=db_ssl,
            postgres_pool_min=db_pool_min,
            postgres_pool_max=db_pool_max,
            tables=",".join(sync_tables) if sync_tables else None,
            wait_for=[wait_period] if full_sync else None,
        )

if __name__ == "__main__":
    arc_db_load_flow(
        db_block_name="local-hasura",
        base_path="./typescript/",
        skip_squash=True,
        skip_view=True,
        full_sync=True,
    )

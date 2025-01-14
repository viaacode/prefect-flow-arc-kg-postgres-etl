from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_meemoo.triplydb.tasks import run_javascript
from prefect_sqlalchemy.credentials import DatabaseCredentials
import os

@flow(
    name="prefect-flow-arc-kg-postgres-etl",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def main_flow(
    triplydb_block_name: str = "triplydb",
    triplydb_owner: str = "meemoo",
    triplydb_dataset: str = "knowledge-graph",
    triplydb_destination_dataset: str = "hetarchief",
    triplydb_destination_graph: str = "hetarchief",
    base_path: str = "/opt/prefect/typescript/",
    script_path: str = "lib/",
    skip_squash: bool = False,
    skip_view: bool = False,
    skip_cleanup: bool = False,
    db_block_name: str = "local",  # "hetarchief-tst",
    db_ssl: bool = True,
    record_limit: int = None,
    batch_size: int = 100,
    full_sync: bool = False,
    debug_mode: bool = False,
    logging_level: str = os.environ.get('PREFECT_LOGGING_LEVEL')
):
    """Flow to query the TriplyDB dataset and update the graphql database.
    Blocks:
        - triplydb (TriplyDBCredentials): Credentials to connect to MediaHaven
        - hetarchief-tst (PostgresCredentials): Credentials to connect to the postgres database
    """
    # Load credentials
    triply_creds = TriplyDBCredentials.load(triplydb_block_name)
    postgres_creds = DatabaseCredentials.load(db_block_name)

    # Figure out start time
    last_modified_date = get_last_run_config("%Y-%m-%d") if not full_sync else None

    # Run javascript
    sync_service_script: str = "index.js"

    run_javascript.with_options(
        name=f"Sync KG to services with {sync_service_script}",
    ).submit(
        script_path=base_path + script_path + sync_service_script,
        base_path=base_path,
        triplydb=triply_creds,
        triplydb_owner=triplydb_owner,
        triplydb_dataset=triplydb_dataset,
        triplydb_destination_dataset=triplydb_destination_dataset,
        triplydb_destination_graph=triplydb_destination_graph,
        skip_squash=skip_squash,
        skip_view=skip_view,
        skip_cleanup=skip_cleanup,
        postgres=postgres_creds,
        record_limit=record_limit,
        batch_size=batch_size,
        since=last_modified_date,
        debug_mode=debug_mode,
        logging_level=logging_level,
        postgres_ssl=db_ssl,
    )

if __name__ == "__main__":
    main_flow(
        triplydb_block_name="triplydb-meemoo",
        db_block_name="local-hasura",
        base_path="./typescript/",
        triplydb_dataset="hetarchief",
        skip_squash=True,
        skip_view=True,
        full_sync=True,
    )

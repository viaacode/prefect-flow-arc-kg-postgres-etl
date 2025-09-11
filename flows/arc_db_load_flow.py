import os
from datetime import datetime

from pendulum.datetime import DateTime
from prefect import flow, get_run_logger, task
from prefect.states import Completed
from prefect.task_runners import ConcurrentTaskRunner
from prefect_meemoo.config.last_run import save_last_run_config
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_meemoo.triplydb.tasks import run_javascript
from prefect_sqlalchemy.credentials import DatabaseCredentials
from psycopg2 import DatabaseError, connect, sql
from psycopg2.extras import RealDictCursor

@flow(
    name="prefect_flow_arc_db_load",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def arc_db_load_flow(
    triplydb_block_name: str = "triplydb",
    triplydb_owner: str = "meemoo",
    triplydb_dataset: str = "knowledge-graph",
    triplydb_destination_dataset: str = "hetarchief",
    triplydb_destination_graph: str = "hetarchief",
    base_path: str = "/opt/prefect/typescript/",
    script_path: str = "lib/",
    db_block_name: str = "local",
    db_ssl: bool = True,
    db_pool_min: int = 0,
    db_pool_max: int = 5,
    db_loading_batch_size: int = 100,
    db_clear_value_tables: list[str] = None,
    record_limit: int = None,
    last_modified: DateTime = None,
    full_sync: bool = False,
    debug_mode: bool = False,
    logging_level: str = os.environ.get("PREFECT_LOGGING_LEVEL"),
):
    """Flow to query the TriplyDB dataset and update the graphql database.
    Blocks:
        - triplydb (TriplyDBCredentials): Credentials to connect to MediaHaven
        - hetarchief-tst (PostgresCredentials): Credentials to connect to the postgres database
    """
    # Load credentials
    triply_creds = TriplyDBCredentials.load(triplydb_block_name)
    postgres_creds = DatabaseCredentials.load(db_block_name)

    # Run javascript which loads graph into postgres
    load_db_script: str = "2_database_load.js"

    loading = run_javascript.with_options(
            name=f"Sync KG to services with {load_db_script}",
        ).submit(
            script_path=base_path + script_path + load_db_script,
            base_path=base_path,
            triplydb=triply_creds,
            triplydb_owner=triplydb_owner,
            triplydb_dataset=triplydb_dataset,
            triplydb_destination_dataset=triplydb_destination_dataset,
            triplydb_destination_graph=triplydb_destination_graph,
            postgres=postgres_creds,
            record_limit=record_limit,
            batch_size=db_loading_batch_size,
            since=last_modified if not full_sync else None,
            debug_mode=debug_mode,
            logging_level=logging_level,
            postgres_ssl=db_ssl,
            postgres_pool_min=db_pool_min,
            postgres_pool_max=db_pool_max,
            db_clear_value_tables=','.join(db_clear_value_tables) if db_clear_value_tables else None,
        )

if __name__ == "__main__":
    arc_db_load_flow(
        triplydb_block_name="triplydb-meemoo",
        db_block_name="local-hasura",
        base_path="./typescript/",
        triplydb_dataset="hetarchief",
        skip_squash=True,
        skip_view=True,
        full_sync=True,
    )

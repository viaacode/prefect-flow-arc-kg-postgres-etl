import os

from pendulum.datetime import DateTime
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from prefect_meemoo.config.last_run import save_last_run_config
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_meemoo.triplydb.tasks import run_javascript


@flow(
    name="prefect_flow_arc_kg_view",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def kg_view_flow(
    triplydb_block_name: str = "triplydb",
    triplydb_owner: str = "meemoo",
    triplydb_dataset: str = "knowledge-graph",
    triplydb_destination_dataset: str = "hetarchief",
    triplydb_destination_graph: str = "hetarchief",
    base_path: str = "/opt/prefect/typescript/",
    prefix_id_base: str = "https://data.hetarchief.be/id/entity/",
    script_path: str = "lib/",
    last_modified: DateTime = None,
    full_sync: bool = False,
    logging_level: str = os.environ.get("PREFECT_LOGGING_LEVEL"),
):
    """Flow to query the TriplyDB dataset and update the graphql database.
    Blocks:
        - triplydb (TriplyDBCredentials): Credentials to connect to MediaHaven
        - hetarchief-tst (PostgresCredentials): Credentials to connect to the postgres database
    """
    # Load credentials
    triply_creds = TriplyDBCredentials.load(triplydb_block_name)

    # Run javascript which constructs the view
    kg_view_script: str = "1_kg_view_construct.js"

    run_javascript.with_options(
        name=f"Construct view with {kg_view_script}",
    ).submit(
        script_path=base_path + script_path + kg_view_script,
        base_path=base_path,
        triplydb=triply_creds,
        triplydb_owner=triplydb_owner,
        triplydb_dataset=triplydb_dataset,
        triplydb_destination_dataset=triplydb_destination_dataset,
        triplydb_destination_graph=triplydb_destination_graph,
        logging_level=logging_level,
        since=last_modified if not full_sync else None,
        prefix_id_base=prefix_id_base,
    )


if __name__ == "__main__":
    kg_view_flow(
        triplydb_block_name="triplydb-meemoo",
        db_block_name="local-hasura",
        base_path="./typescript/",
        triplydb_dataset="hetarchief",
        skip_squash=True,
        skip_view=True,
        full_sync=True,
    )

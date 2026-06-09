from glob import glob
import os

from pendulum.datetime import DateTime
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_meemoo.config.last_run import save_last_run_config
from SPARQLWrapper import SPARQLWrapper
from rdflib import Literal

@task
def exec_sparql(endpoint, query_name, query_string, **kwargs):
    
    sparql = SPARQLWrapper(endpoint)

    # Perform variable replacement if parameters are provided
    processed_query = query_string
    if kwargs:
        for var, value in kwargs.items():
            # Replace ?variableName with the actual value (assuming literal format for value)
            # TODO: allow passing URIs or customizing datatype
            processed_query = processed_query.replace(f"?{var}", Literal(value).n3())

    sparql.setQuery(processed_query)

    with open(f"{query_name}.ttl", "w") as result_file:
        # Write results to file
        result_file.write(sparql.query())
    

@flow(
    name="prefect_flow_arc_kg_view",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def kg_view_flow(
    endpoint_block: str = "sparql-endpoint",
    base_path: str = "/opt/prefect/typescript/",
    prefix_id_base: str = "https://data.hetarchief.be/id/entity/",
    last_modified: DateTime = None,
    full_sync: bool = False,
    or_ids: list[str] = None,
):
    """Flow to query the TriplyDB dataset and update the graphql database.
    Blocks:
        - triplydb (TriplyDBCredentials): Credentials to connect to MediaHaven
        - hetarchief-tst (PostgresCredentials): Credentials to connect to the postgres database
    """
    logger = get_run_logger()

    # Load credentials
    endpoint = Secret.load(endpoint_block)

      # Find all relevant SPARQL files
    sparql_files = glob(os.path.join(base_path + "queries", "*.sparql"))

    # Create and submit a task for each .sparql file
    sparql_tasks = []
    for file_path in sparql_files:
        query_name = os.path.splitext(os.path.basename(file_path))[0]
        query_string = ""

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                query_string = f.read()
        except Exception as e:
            logger.exception(f"Error reading {file_path}")
            continue

        # Submit exec_sparql task for this file
        task_result = exec_sparql.with_options(
            name=f"Execute SPARQL for {query_name}",
        ).submit(
            endpoint=endpoint,
            query_name=query_name,
            query_string=query_string,
            since=last_modified if not full_sync else None,
            or_ids=','.join(or_ids) if or_ids else None,
            prefix_id_base=prefix_id_base,
        )
        sparql_tasks.append(task_result)


if __name__ == "__main__":
    kg_view_flow(
        endpoint_block="sparql-meemoo",
        db_block_name="local-hasura",
        base_path="./typescript/",
        triplydb_dataset="hetarchief",
        skip_squash=True,
        skip_view=True,
        full_sync=True,
    )

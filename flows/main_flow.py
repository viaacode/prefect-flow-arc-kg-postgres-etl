from enum import Enum
from prefect.states import Failed, Completed
import psycopg2
from prefect import flow, task, get_run_logger
from prefect.deployments import run_deployment
from prefect.task_runners import ConcurrentTaskRunner
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_meemoo.triplydb.tasks import run_javascript
from prefect_sqlalchemy.credentials import DatabaseCredentials
import os
from psycopg2.extras import RealDictCursor
from datetime import datetime


def get_min_date(format="%Y-%m-%dT%H:%M:%S.%fZ"):
    return datetime.min.strftime(format)


# Run a deployment as a task
@task(task_run_name="Run deployment {flow_name}/{deployment_name}")
def run_deployment_task(flow_name: str, deployment_name: str, parameters: dict):
    flow_run = run_deployment(
        name=f"{flow_name}/{deployment_name}", parameters=parameters
    )
    return flow_run.state


@task
def populate_index_table(db_credentials: DatabaseCredentials, since: str = None):
    logger = get_run_logger()

    # Connect to ES and Postgres
    logger.info("(Re)connecting to postgres")
    db_conn = psycopg2.connect(
        user=db_credentials.username,
        password=db_credentials.password.get_secret_value(),
        host=db_credentials.host,
        port=db_credentials.port,
        database=db_credentials.database,
        cursor_factory=RealDictCursor,
    )
    db_conn.autocommit = False

    # Create cursor
    cursor = db_conn.cursor()

    # Get list of partitions
    cursor.execute(
        """
        select 
        distinct(ie.schema_maintainer) as id, 
        count(*) as cnt
        from
        graph.intellectual_entity ie 
        group by 1 
        order by 2 ASC
        """,
    )
    partitions = cursor.fetchall()
    for row in partitions:
        partition = row["id"]
        count = row["cnt"]

        failed = []
        try:
            # Run query
            query_vars = {
                "partition": partition,
                "since": since if since is not None else get_min_date(),
            }

            logger.info(
                "Start populating index_documents table for partition %s since %s (%s records).",
                partition,
                query_vars["since"],
                count,
            )

            # Delete the Intellectual Entities
            cursor.execute(
                "select graph.update_index_documents_per_cp(%(partition)s,%(since)s);",
                query_vars,
            )
            logger.info(
                "Populated index_documents partition %s (%s records).",
                partition,
                cursor.rowcount,
            )
            # Commit your changes in the database
            db_conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(
                "Error while populating partition %s; rolling back. ",
                partition,
                error,
            )
            db_conn.rollback()
            failed.append(partition)

    # closing database connection.
    if db_conn:
        cursor.close()
        db_conn.close()
        logger.info("PostgreSQL connection is closed")

    total = len(partitions)
    failed_count = len(failed)
    if failed_count > 0:
        return Failed(message=f"Failed to populate {failed_count}/{total} partitions: {failed}.")
    return Completed(message=f"Batch succeeded: {total} partitions populated.")


@task
def delete_records_from_db(
    db_credentials: DatabaseCredentials,
):
    logger = get_run_logger()

    # Connect to ES and Postgres
    logger.info("(Re)connecting to postgres")
    db_conn = psycopg2.connect(
        user=db_credentials.username,
        password=db_credentials.password.get_secret_value(),
        host=db_credentials.host,
        port=db_credentials.port,
        database=db_credentials.database,
        cursor_factory=RealDictCursor,
    )
    db_conn.autocommit = False

    try:
        # Create server-side cursor
        cursor = db_conn.cursor()

        # Run query

        # Delete the Intellectual Entities
        cursor.execute(
            """
        DELETE FROM graph."intellectual_entity" x
        USING graph."mh_fragment_identifier" y
        WHERE y.intellectual_entity_id = x.id AND y.is_deleted;"""
        )
        logger.info(
            "Deleted the Intellectual Entities from database(%s records)",
            cursor.rowcount,
        )

        # Delete the fragment entries in database
        cursor.execute('DELETE FROM graph."mh_fragment_identifier" WHERE is_deleted;')
        logger.info(
            'Deleted the fragments from table graph."mh_fragment_identifier" (%s records)',
            cursor.rowcount,
        )

        # Commit your changes in the database
        db_conn.commit()
        logger.info("Database deletes succesful")

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(
            "Error in transction Reverting all other operations of a transction ", error
        )
        db_conn.rollback()

    finally:
        # closing database connection.
        if db_conn:
            cursor.close()
            db_conn.close()
            logger.info("PostgreSQL connection is closed")


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
    skip_load: bool = False,
    skip_indexing: bool = False,
    es_block_name: str = "arc-elasticsearch",
    es_chunk_size: int = 500,
    es_request_timeout: int = 30,
    es_max_retries: int = 10,
    es_retry_on_timeout: bool = True,
    db_indexing_batch_size: int = 500,
    db_block_name: str = "local",
    db_ssl: bool = True,
    db_pool_min: int = 0,
    db_pool_max: int = 5,
    db_loading_batch_size: int = 100,
    record_limit: int = None,
    full_sync: bool = False,
    debug_mode: bool = False,
    logging_level: str = os.environ.get("PREFECT_LOGGING_LEVEL"),
    flow_name_indexer: str = "prefect-flow-arc-indexer",
    deployment_name_indexer: str = "prefect-flow-arc-indexer-int",
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

    # Run javascript which loads graph into postgres
    sync_service_script: str = "index.js"

    loading = run_javascript.with_options(
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
        skip_load=skip_load,
        postgres=postgres_creds,
        record_limit=record_limit,
        batch_size=db_loading_batch_size,
        since=last_modified_date,
        debug_mode=debug_mode,
        logging_level=logging_level,
        postgres_ssl=db_ssl,
        postgres_pool_min=db_pool_min,
        postgres_pool_max=db_pool_max,
    )

    # Populate the index table
    populating = populate_index_table.submit(
        db_credentials=postgres_creds,
        since=last_modified_date,
        wait_for=loading,
    )

    # Run the indexer
    run_deployment_task.submit(
        flow_name=flow_name_indexer,
        deployment_name=deployment_name_indexer,
        parameters={
            "db_block_name": db_block_name,
            "db_table": "graph.index_documents",
            "es_block_name": es_block_name,
            "full_sync": full_sync,
            "es_chunk_size": es_chunk_size,
            "es_request_timeout": es_request_timeout,
            "es_max_retries": es_max_retries,
            "es_retry_on_timeout": es_retry_on_timeout,
            "db_batch_size": db_indexing_batch_size,
        },
        wait_for=populating,
    ) if not skip_indexing else None

    # Delete all records from database
    delete_records_from_db.submit(
        db_credentials=postgres_creds, wait_for=[loading, populating]
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

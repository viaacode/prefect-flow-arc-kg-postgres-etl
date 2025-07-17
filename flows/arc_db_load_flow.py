import os
from datetime import datetime
from enum import Enum

from pendulum.datetime import DateTime
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment
from prefect.states import Completed, Failed
from prefect.task_runners import ConcurrentTaskRunner
from prefect_meemoo.config.last_run import (get_last_run_config,
                                            save_last_run_config)
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_meemoo.triplydb.tasks import run_javascript
from prefect_sqlalchemy.credentials import DatabaseCredentials
from psycopg2 import DatabaseError, connect, sql
from psycopg2.extras import RealDictCursor


def get_min_date(format="%Y-%m-%dT%H:%M:%S.%fZ"):
    return datetime.min.strftime(format)



@task
def populate_index_table(db_credentials: DatabaseCredentials, since: str = None):
    logger = get_run_logger()

    # Connect to ES and Postgres
    logger.info("(Re)connecting to postgres")
    db_conn = connect(
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
        SELECT 
        ie.schema_maintainer as id, 
        lower(replace(org_identifier, '-','_')) as partition,
        count(*) as cnt
        FROM graph.intellectual_entity ie 
        JOIN graph.organization o ON ie.schema_maintainer = o.id
        GROUP BY 1,2 
        ORDER BY 3 ASC
        """,
    )
    partitions = cursor.fetchall()
    failed = []
    last_error = None
    ids = []
    for row in partitions:
        partition = row["partition"]
        count = row["cnt"]
        ids.append(row["id"])
        try:
            # Try to create partition
            create_query = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {db_table} PARTITION OF graph.index_documents FOR VALUES IN (%(id)s);"
                ).format(
                db_table=sql.Identifier("graph", partition),
            )
            cursor.execute(create_query, {"id": row["id"]})
            logger.info(
                "Created partition %s in index_documents table.",
                partition,
            )

            # when full sync, truncate partition first
            if since is None:
                sql_query = sql.SQL("TRUNCATE {db_table};").format(
                    db_table=sql.Identifier("graph", partition),
                )
                cursor.execute(sql_query)
                logger.info(
                    "Truncated partition %s in index_documents table.",
                    partition,
                )
            # Run query
            query_vars = {
                "id": row["id"],
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
                "select graph.update_index_documents_per_cp(%(id)s,%(since)s);",
                query_vars,
            )
            logger.info(
                "Populated index_documents partition %s (%s records).",
                partition,
                cursor.rowcount,
            )
            # Commit your changes in the database
            db_conn.commit()

        except (Exception, DatabaseError) as error:
            logger.error(
                "Error while populating partition %s; rolling back. ",
                partition,
                error,
            )
            db_conn.rollback()
            last_error = error
            failed.append(partition)
        else:
            logger.info(
                "Partition %s populated successfully with %s records.",
                partition,
                cursor.rowcount,
            )

    # Drop partitions that are no longer there
    if since is None:
        # Get all partitions that were not touched
        cursor.execute(
            """
            SELECT lower(replace(index, '-','_')) as partition
            FROM graph.index_documents 
            WHERE index NOT IN(%(id)s)
            """,
            {"id": ids}
        )
        deleted_partitions = cursor.fetchall()
        logger.info("Cleaning partitions that are no longer there: %s", list(deleted_partitions))
        for row in deleted_partitions:
            partition = row["partition"]
            try:
                sql_query = sql.SQL("DROP {db_table};").format(
                    db_table=sql.Identifier("graph", partition),
                )
                cursor.execute(sql_query)
                logger.info(
                    "Dropped partition %s in index_documents table.",
                    partition,
                )
                # Commit your changes in the database
                db_conn.commit()

            except (Exception, DatabaseError) as error:
                logger.error(
                    "Error while populating partition %s; rolling back. ",
                    partition,
                    error,
                )
                db_conn.rollback()

    
    
    # closing database connection.
    if db_conn:
        cursor.close()
        db_conn.close()
        logger.info("PostgreSQL connection is closed")

    total = len(partitions)
    failed_count = len(failed)
    if failed_count > 0:
        logger.error(f"Failed to populate {failed_count}/{total} partitions: {failed}.")
        raise last_error
        return Failed(
            message=f"Failed to populate {failed_count}/{total} partitions: {failed}."
        )
    return Completed(message=f"Batch succeeded: {total} partitions populated.")


@task
def delete_records_from_db(
    db_credentials: DatabaseCredentials,
):
    logger = get_run_logger()

    # Connect to ES and Postgres
    logger.info("(Re)connecting to postgres")
    db_conn = connect(
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

    except (Exception, DatabaseError) as error:
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
    skip_load: bool = False,
    db_block_name: str = "local",
    db_ssl: bool = True,
    db_pool_min: int = 0,
    db_pool_max: int = 5,
    db_loading_batch_size: int = 100,
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
    load_db_script: str = "3_database_load.js"

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
    ) if not skip_load else None

    # Populate the index table
    populating = populate_index_table.submit(
        db_credentials=postgres_creds,
        since=last_modified if not full_sync else None,
        wait_for=loading,
    )

    # Delete all records from database
    delete_records_from_db.submit(
        db_credentials=postgres_creds, wait_for=[loading, populating]
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

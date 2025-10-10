from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_meemoo.config.last_run import save_last_run_config
from prefect_sqlalchemy.credentials import DatabaseCredentials
from psycopg2 import DatabaseError, connect, sql
from psycopg2.extras import RealDictCursor
from flows.arc_db_load_index_tables_flow import get_partitions

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

    except (Exception, DatabaseError):
        logger.exception(
            "Error in transction Reverting all other operations of a transction.",
        )
        db_conn.rollback()
        raise

    finally:
        # closing database connection.
        if db_conn:
            cursor.close()
            db_conn.close()
            logger.info("PostgreSQL connection is closed")

@task
def drop_partition(
    db_credentials: DatabaseCredentials, partition: str
) -> None:
    """Drop a partition (used in full sync)."""
    logger = get_run_logger()
    try:
        with connect(
            user=db_credentials.username,
            password=db_credentials.password.get_secret_value(),
            host=db_credentials.host,
            port=db_credentials.port,
            database=db_credentials.database,
        ) as db_conn:
            with db_conn.cursor() as cursor:
                drop_query = sql.SQL("DROP TABLE IF EXISTS {db_table};").format(
                    db_table=sql.Identifier("graph", partition)
                )
                cursor.execute(drop_query)
                db_conn.commit()
                logger.info("Dropped partition %s", partition)

    except (Exception, DatabaseError):
        logger.exception("Error dropping partition %s", partition)
        db_conn.rollback()
        raise

def delete_index_tables(
    db_credentials: DatabaseCredentials,
    indexes: list[str],
):
    logger = get_run_logger()
    # Connect to Postgres
    with connect(
        user=db_credentials.username,
        password=db_credentials.password.get_secret_value(),
        host=db_credentials.host,
        port=db_credentials.port,
        database=db_credentials.database,
        cursor_factory=RealDictCursor,
    ) as db_conn:
        # Create cursor
        with db_conn.cursor() as cursor:
            # Get all partitions that do have any records anymore
            cursor.execute(
                """
                SELECT DISTINCT lower(replace(index, '-','_')) as partition
                FROM graph.index_documents 
                WHERE index NOT IN %(indexes)s
                """,
                {"indexes": tuple(indexes)}
            )
            deleted_partitions = cursor.fetchall()
    for row in deleted_partitions:
        deleted_partition = row["partition"]
        drop_partition.with_options(
            name=f"Drop partition {deleted_partition}",
        ).submit(
            db_credentials=db_credentials,
            partition=deleted_partition,
        )

@flow(
    name="prefect_flow_arc_db_deletes",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def arc_db_delete_flow(
    db_block_name: str,
):

    postgres_creds = DatabaseCredentials.load(db_block_name)
    partitions = get_partitions(db_credentials=postgres_creds)
    indexes = [row["index"] for row in partitions]

    delete_index_tables(
        db_credentials=postgres_creds,
        indexes=indexes,
    )
    delete_records_from_db.submit(
        db_credentials=postgres_creds
    )


    
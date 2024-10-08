from prefect import task, flow, get_run_logger
from prefect.server.schemas.states import Failed
import requests
import psycopg2
from psycopg2.extras import LoggingConnection
from io import StringIO
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_sqlalchemy.credentials import DatabaseCredentials
from prefect.blocks.system import JSON
from prefect_meemoo.config.last_run import (get_last_run_config,
                                            save_last_run_config)
from prefect.task_runners import ConcurrentTaskRunner

@task(task_run_name="upsert_pages-{table_name}")
def upsert_pages(
    table_name: str, 
    csv_url: str, 
    postgres_credentials: DatabaseCredentials, 
    triply_credentials:TriplyDBCredentials, 
    since: str = None,
    dedupe: bool = False):
    # Load logger
    logger = get_run_logger()

    def get_next_page_link(response):
        """Extract the next page link from the Link header if it exists."""
        link_header = response.headers.get("Link", None)
        logger.debug(response.headers)
        if link_header:
            links = requests.utils.parse_header_links(link_header.strip("<>"))
            for link in links:
                if link.get("rel") == "next":
                    return link["url"]
        return None

    def fetch_and_insert_page(url, cursor, temp_table_name):
        """Fetch a single page of CSV data and insert it into the temporary table. Returns the nex page url if there is one."""
        logger.info(f"Fetch and insert page {url} into {temp_table_name}")
        params = {"since": since.isoformat()} if since is not None else {}
        response = requests.get(
            url,
            params=params,
            headers={
                "Authorization": "Bearer " + triply_credentials.token.get_secret_value(),
                "Accept": "text/csv",
            },
        )
        response.raise_for_status()

        csv_data = StringIO(response.text)
        copy_query = f"""
        COPY {temp_table_name} FROM STDIN WITH CSV HEADER DELIMITER ',';
        """
        cursor.copy_expert(copy_query, csv_data)
        return get_next_page_link(response=response)

    # Step 1: Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
        #connection_factory=LoggingConnection, 
    )
    # disable autocommit mode
    conn.autocommit = False

    cur = conn.cursor()

    # Step 2: Create a temporary table for upserting. Exclude all indexes to deal with duplicate rows
    table_no_schema = table_name.split('.',1)[1]
    temp_table_name = f"temp_{table_no_schema}"
    create_temp_table_query = f"""
    DROP TABLE IF EXISTS {temp_table_name};
    CREATE TEMP TABLE {temp_table_name} (LIKE {table_name} INCLUDING ALL EXCLUDING INDEXES);
    """
    cur.execute(create_temp_table_query)
    conn.commit()

    try:

        # Step 3: Fetch and insert each page of the CSV data
        url = csv_url
        while url:
            url = fetch_and_insert_page(url, cur, temp_table_name)
            conn.commit()

        # Step 4: Upsert from the temporary table to the actual table
        logger.info(f"Upsert from the temporary table {temp_table_name} to the actual table {table_name}")

        # Get column names
        get_columns_query = f"""
        SELECT COLUMN_NAME from information_schema.columns 
        WHERE table_name='{table_no_schema}'
        """
        cur.execute(get_columns_query)
        column_names = [row[0] for row in cur] 

        # Get primary keys
        get_primary_keys_query = f"""
        SELECT COLUMN_NAME from information_schema.key_column_usage 
        WHERE table_name='{table_no_schema}' AND constraint_name LIKE '%pkey'
        """
        cur.execute(get_primary_keys_query)
        primary_keys = [row[0] for row in cur]

        # Dedupe temp table
        if dedupe:
            join_map = list(map(lambda cn: f"a.{cn} = b.{cn}", primary_keys))
            return_map = list(map(lambda cn: f"a.{cn}", primary_keys))
            delete_duplicates = f"""
            WITH dupes AS (
                SELECT {', '.join(primary_keys)}, ROW_NUMBER() OVER(
                        PARTITION BY {', '.join(primary_keys)}
                        ORDER BY {', '.join(primary_keys)}
                    ) AS row_num
                FROM (
                    SELECT DISTINCT * FROM {temp_table_name}
                ) x
            )
            DELETE FROM {temp_table_name} a
            USING dupes b
            WHERE b.row_num > 2 AND {' AND '.join(join_map)}
            RETURNING {', '.join(return_map)}, b.row_num
            """
            logger.debug(f"Executing delete query {delete_duplicates}")
            cur.execute(delete_duplicates)
            rows_deleted = cur.rowcount
            logger.info(f"Dedupe {rows_deleted} rows from temporary table {temp_table_name}")
            logger.debug(f"Deleted rows from {temp_table_name}: {cur.fetchall()}")
            conn.commit()

        column_map = list(map(lambda cn: f"{cn} = EXCLUDED.{cn}", column_names))

        # When full sync: truncate table first
        if since is None:
            truncate_query = f"""
            TRUNCATE {table_name} CASCADE
            """
            logger.info(f"Truncating {table_name} because full sync is enabled.")
            cur.execute(truncate_query)
        
        # Upsert all rows from temp table. Use distinct to deal with possible duplicates
        upsert_query = f"""
        INSERT INTO {table_name}
        SELECT DISTINCT ON({', '.join(primary_keys)}) * FROM {temp_table_name}
        ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE
        SET {', '.join(column_map)};
        """
        logger.debug(f"Executing upsert query {upsert_query}")
        cur.execute(upsert_query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error("Error in transaction, reverting all changes using rollback ", error)
        conn.rollback()
        return Failed(message=f"Table {table_name} not successfully synced; error in transaction.")
 
    finally:
        # closing database connection.
        if conn:

            # Step 5: Clean up and close the connection
            cur.close()
            conn.close()
            logger.info("PostgreSQL database connection is closed")

    return logger.info(f"Table {table_name} successfully synced.")


@flow(name="prefect-flow-arc-kg-postgres-etl", task_runner=ConcurrentTaskRunner(),  on_completion=[save_last_run_config])
def main_flow(
    triplydb_block_name: str = "triplydb",
    postgres_block_name: str = "local",#"hetarchief-tst",
    config_block_name: str = "saved-query-config",
    full_sync: bool = False,
    dedupe: bool = False
):
    """
        Flow to query the TriplyDB dataset and update the graphql database.
        Blocks:
            - triplydb (TriplyDBCredentials): Credentials to connect to MediaHaven
            - hetarchief-tst (PostgresCredentials): Credentials to connect to the postgres database
            - saved-query-config (JSON): JSON object mapping postgres table to TriplyDB saved query run link. JSON must be of format { [table_name]: { url: runlink, position: ...}}
    """
    # Load logger
    logger = get_run_logger()

    # Load configuration
    table_config = JSON.load(config_block_name)

    # Load credentials
    triply_creds = TriplyDBCredentials.load(triplydb_block_name)
    postgres_creds = DatabaseCredentials.load(postgres_block_name)

    # Figure out start time
    if not full_sync:
        last_modified_date = get_last_run_config("%Y-%m-%d")

    # For each entry in config table: start sync task
    tasks = {}
    for table_name, csv_url in sorted(table_config.value.items(), key=lambda t: t[1]["position"]):
        dependencies = tasks[csv_url["position"] - 1] if csv_url["position"] - 1 in tasks else []
        task = upsert_pages.submit(
            table_name=table_name,
            csv_url=csv_url["url"],
            triply_credentials=triply_creds,
            postgres_credentials=postgres_creds,
            since=last_modified_date if not full_sync else None,
            wait_for=dependencies,
            dedupe=dedupe
        )
        # Initialize entry in task dict if not exists
        if csv_url["position"] not in tasks:
            tasks[csv_url["position"]] = []
        # append to array
        tasks[csv_url["position"]].append(task)

if __name__ == "__main__":
    main_flow()
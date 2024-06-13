import datetime
from prefect import task, flow, get_run_logger
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
    since: datetime=None):
    # Load logger
    logger = get_run_logger()

    def get_next_page_link(response):
        """Extract the next page link from the Link header if it exists."""
        link_header = response.headers.get("Link", None)
        if link_header:
            links = requests.utils.parse_header_links(link_header.strip("<>"))
            for link in links:
                if link.get("rel") == "next":
                    return link["url"]
        return None

    def fetch_and_insert_page(url, cursor, temp_table_name):
        """Fetch a single page of CSV data and insert it into the temporary table."""
        params = {"since": since.isoformat()} if since is not None else {}
        logger.info(f"Fetch and insert page {url} into {table_name}")
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

    # Step 1: Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
        #connection_factory=LoggingConnection, 
    )
    cur = conn.cursor()

    # Step 2: Create a temporary table for upserting
    temp_table_name = f"temp_{table_name.split('.',1)[1]}"
    create_temp_table_query = f"""
    CREATE TEMP TABLE {temp_table_name} (LIKE {table_name} INCLUDING ALL);
    """
    cur.execute(create_temp_table_query)
    conn.commit()

    # Step 3: Fetch and insert each page of the CSV data
    url = csv_url
    while url:
        fetch_and_insert_page(url, cur, temp_table_name)
        conn.commit()
        url = get_next_page_link(requests.get(url))

    # Step 4: Upsert from the temporary table to the actual table
    logger.info(f"Upsert from the temporary table {temp_table_name} to the actual table {table_name}")

    # Get column names
    get_columns_query = f"""
    SELECT COLUMN_NAME from information_schema.columns 
    WHERE table_name='{temp_table_name}'
    """
    cur.execute(get_columns_query)
    column_names = [row[0] for row in cur] 

    # Get primary keys
    get_primary_keys_query = f"""
    SELECT COLUMN_NAME from information_schema.key_column_usage 
    WHERE table_name='{temp_table_name}'
    """
    cur.execute(get_primary_keys_query)
    primary_keys = [row[0] for row in cur]

    column_map = list(map(lambda cn: f"{cn} = EXCLUDED.{cn}",column_names))

    upsert_query = f"""
    INSERT INTO {table_name}
    SELECT * FROM {temp_table_name}
    ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE
    SET {', '.join(column_map)};
    """
    cur.execute(upsert_query)
    conn.commit()

    # Step 5: Clean up and close the connection
    cur.close()
    conn.close()

    return logger.info(f"Table {table_name} successfully synced.")


@flow(name="prefect-flow-arc-kg-postgres-etl", task_runner=ConcurrentTaskRunner(),  on_completion=[save_last_run_config])
def main_flow(
    triplydb_block_name: str = "triplydb",
    postgres_block_name: str = "local",#"hetarchief-tst",
    config_block_name: str = "saved-query-config",
    full_sync: bool = False
):
    """
        Flow to query the TriplyDB dataset and update the graphql database.
        Blocks:
            - triplydb (TriplyDBCredentials): Credentials to connect to MediaHaven
            - hetarchief-tst (PostgresCredentials): Credentials to connect to the postgres database
            - saved-query-config (JSON): JSON object mapping postgres table to TriplyDB saved query run link
    """
    # Load logger
    logger = get_run_logger()

    # Load configuration
    table_config = JSON.load(config_block_name)

    # Load credentials
    triply_creds = TriplyDBCredentials.load(triplydb_block_name)
    postgres_creds = DatabaseCredentials.load(postgres_block_name)

    # Figure out start time
    last_modified_date = get_last_run_config("%Y-%m-%d")

    # For each entry in config table: start sync task
    for table_name, csv_url in table_config.value.items():
        upsert_pages.submit(
            table_name=table_name,
            csv_url=csv_url,
            triply_credentials=triply_creds,
            postgres_credentials=postgres_creds,
            since=last_modified_date,
        )

if __name__ == "__main__":
    main_flow()
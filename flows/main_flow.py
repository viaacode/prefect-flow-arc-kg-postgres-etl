from prefect import task, flow, get_run_logger
import requests
import psycopg2
from io import StringIO
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_sqlalchemy.credentials import DatabaseCredentials
import datetime

@task()
def upsert_pages(table_name, csv_url, postgres_creds, triply_creds, since = None):
    # Load logger
    logger = get_run_logger()

    def get_next_page_link(response):
        """Extract the next page link from the Link header if it exists."""
        link_header = response.headers.get('Link', None)
        if link_header:
            links = requests.utils.parse_header_links(link_header.strip('<>'))
            for link in links:
                if link.get('rel') == 'next':
                    return link['url']
        return None

    def fetch_and_insert_page(url, cursor, temp_table_name):
        """Fetch a single page of CSV data and insert it into the temporary table."""
        params = {"since": since.isoformat()} if since is not None else {}
        response = requests.get(url, params=params, headers={
            "Authorization": "Bearer " + triply_creds.token,
            "Accept": "text/csv"
        })
        response.raise_for_status()

        csv_data = StringIO(response.text)
        copy_query = f"""
        COPY {temp_table_name} FROM STDIN WITH CSV HEADER DELIMITER ',';
        """
        cursor.copy_expert(copy_query, csv_data)

    # Step 1: Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**postgres_creds)
    cur = conn.cursor()

    # Step 2: Create a temporary table for upserting
    temp_table_name = f'temp_{table_name}'
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
    upsert_query = f"""
    INSERT INTO {table_name}
    SELECT * FROM {temp_table_name}
    ON CONFLICT (id) DO UPDATE
    SET column1 = EXCLUDED.column1,
        column2 = EXCLUDED.column2;  -- Adjust columns as needed
    """
    cur.execute(upsert_query)
    conn.commit()

    # Step 5: Clean up and close the connection
    cur.close()
    conn.close()

    return logger.info("Data inserted successfully.")

@flow(name="FILL_IN_FLOW_NAME_HERE!")
def main_flow(
    triplydb_block_name: str = "triplydb",
    postgres_block_name: str = "hasura",
    since: datetime = None,
    ):
    """
    Here you write your main flow code and call your tasks and/or subflows.
    """

    # Load logger
    logger = get_run_logger()
    
    # Configuration
    table_config = {
        'graph.intellectual_entity': 'https://api.meemoo.triply.cc/queries/meemoo/Query/11/run'
    }

    # Load credentials
    triply_creds = TriplyDBCredentials.load(triplydb_block_name)
    postgres_creds = DatabaseCredentials.load(postgres_block_name)

    # For each entry in config table: start sync task
    for table_name, csv_url in table_config.items():
        upsert_pages(table_name=table_name, csv_url=csv_url, triply_creds=triply_creds, postgres_creds=postgres_creds, since=since).submit()





from prefect import flow, task, get_run_logger
from prefect_sqlalchemy.credentials import DatabaseCredentials
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config
from prefect.task_runners import ConcurrentTaskRunner

import os
import psycopg2
from prefect_aws.s3 import s3_upload
from prefect_aws import AwsCredentials, AwsClientParameters

from flows.convert_alto_to_simplified_json import (
    SimplifiedAlto,
    convert_alto_xml_url_to_simplified_json,
)


# Task to execute SPARQL query via API call and get file list
@task()
def get_url_list(
    postgres_credentials: DatabaseCredentials,
    since: str = None,
) -> list[tuple[str, str]]:
    logger = get_run_logger()

    sql_query = """
    SELECT representation_id, premis_stored_at
    FROM graph.file f
    JOIN graph.includes i ON i.file_id = f.id
    WHERE f.ebucore_has_mime_type IN ('application/xml', 'text/plain') AND schema_name LIKE '%alto%'
    """

    if since is not None:
        sql_query += f" AND f.updated_at >= {since}"

    # Step 1: Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
        # connection_factory=LoggingConnection,
    )
    logger.info(f"Executing query on {postgres_credentials.host}: {sql_query}")
    cur = conn.cursor()
    cur.execute(sql_query)
    url_list = cur.fetchall()
    logger.info(f"Retrieved {len(url_list)} URLs.")
    return url_list


# Task to run the Node.js script and capture stdout as JSON
@task
def create_transcript(url: str):
    logger = get_run_logger()

    try:
        return (
            f"{os.path.basename(url)}.json",
            convert_alto_xml_url_to_simplified_json(url),
        )
    except Exception as e:
        logger.error(f"Failed to process {url}: {str(e)}")
        raise e


@task(task_run_name="insert-{s3_url}-in-database")
def insert_schema_transcript(
    representation_id: str,
    s3_url: str,
    alto_json: SimplifiedAlto,
    postgres_credentials: DatabaseCredentials,
):
    logger = get_run_logger()

    # connect to database
    conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
        # connection_factory=LoggingConnection,
    )
    cur = conn.cursor()
    logger.info(
        f"Updating transcript in 'graph.representation' for {representation_id}"
    )
    # insert transcript into table
    cur.execute(
        "UPDATE graph.representation SET schema_transcript = %s WHERE id = %s",
        (alto_json.to_transcript(), representation_id),
    )
    # insert url into table
    logger.info(
        f"Inserting {s3_url} into 'graph.schema_transcript_url' for {representation_id}"
    )
    cur.execute(
        """
        INSERT INTO graph.schema_transcript_url (representation_id, schema_transcript_url) 
        VALUES (%s, %s) 
        ON CONFLICT(representation_id) 
        DO UPDATE SET schema_transcript_url = EXCLUDED.schema_transcript_url;
        """,
        (representation_id, s3_url),
    )
    conn.commit()

    # Step 5: Clean up and close the connection
    cur.close()
    conn.close()


@flow(
    name="prefect_flow_arc_alto_to_json",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def arc_alto_to_json(
    s3_endpoint: str = "http://assets-int.hetarchief.be",
    s3_bucket_name: str = "hetarchief",
    s3_block_name: str = "arc-object-store",
    db_block_name: str = "local",
    full_sync: bool = False,
):
    # Load credentials
    postgres_creds = DatabaseCredentials.load(db_block_name)
    s3_creds = AwsCredentials.load(s3_block_name)
    client_parameters = AwsClientParameters(endpoint_url=s3_endpoint)

    # Figure out start time
    if not full_sync:
        last_modified_date = get_last_run_config("%Y-%m-%d")

    url_list = get_url_list(
        postgres_creds,
        since=last_modified_date if not full_sync else None,
    )

    for representation_id, url in url_list:
        transcript = create_transcript.submit(url=url)

        if not transcript.wait().is_failed():
            key, alto_json = transcript.result()

            s3_key = s3_upload.submit(
                bucket=s3_bucket_name,
                key=key,
                data=str(alto_json).encode(),
                aws_credentials=s3_creds,
                aws_client_parameters=client_parameters,
                wait_for=transcript,
            )

            insert_schema_transcript.submit(
                representation_id=representation_id,
                s3_url=f"{s3_endpoint}/{s3_bucket_name}/{s3_key.result()}",
                alto_json=alto_json,
                postgres_credentials=postgres_creds,
                wait_for=s3_key,
            )

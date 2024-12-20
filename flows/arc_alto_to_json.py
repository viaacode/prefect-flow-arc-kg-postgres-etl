from prefect import flow, task, get_run_logger
from prefect_sqlalchemy.credentials import DatabaseCredentials
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config
from prefect.task_runners import ConcurrentTaskRunner

import os
import psycopg2
from prefect_aws import AwsCredentials, AwsClientParameters

from flows.convert_alto_to_simplified_json import (
    SimplifiedAlto,
    convert_alto_xml_url_to_simplified_json,
)


# Task to execute SPARQL query via API call and get file list
@task
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


@task
def create_and_upload_transcript_batch(
    batch: list[str, str],
    s3_bucket_name: str,
    s3_credentials: AwsCredentials,
    s3_client_parameters: AwsClientParameters = AwsClientParameters(),
):
    logger = get_run_logger()

    try:
        output = []
        for representation_id, url in batch:
            transcript: SimplifiedAlto = convert_alto_xml_url_to_simplified_json(url)
            s3_key = f"{os.path.basename(url)}.json"

            logger.info(
                "Uploading object to bucket %s with key %s", s3_bucket_name, key
            )

            s3_client = s3_credentials.get_boto3_session().client(
                "s3", **s3_client_parameters.get_params_override()
            )

            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=s3_key,
                Body=str(transcript).encode("utf-8"),
            )

            output.append(
                (
                    representation_id,
                    f"{s3_client_parameters.endpoint_url}/{s3_bucket_name}/{s3_key}",
                    transcript.to_transcript(),
                )
            )
        return output

    except Exception as e:
        logger.error(f"Failed to process batch: {str(e)}")
        raise e


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


@task
def insert_schema_transcript_batch(
    batch: list[str, str, str],
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
    logger.info(f"Updating {len(batch)} transcripts in 'graph.representation'")
    # insert transcript into table
    update_query = (
        "UPDATE graph.representation SET schema_transcript = %s WHERE id = %s"
    )
    psycopg2.extras.execute_values(
        cur,
        update_query,
        map(lambda item: (item[2], item[0]), batch),
        template=None,
        page_size=100,
    )

    # insert url into table
    logger.info(f"Inserting {len(batch)} URLs into 'graph.schema_transcript_url'.")
    insert_query = """
        INSERT INTO graph.schema_transcript_url (representation_id, schema_transcript_url) 
        VALUES (%s, %s) 
        ON CONFLICT(representation_id) 
        DO UPDATE SET schema_transcript_url = EXCLUDED.schema_transcript_url;
        """
    psycopg2.extras.execute_values(
        cur,
        insert_query,
        map(lambda item: (item[0], item[1]), batch),
        template=None,
        page_size=100,
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
    batch_size: int = 100,
    full_sync: bool = False,
):
    # Load credentials
    postgres_creds = DatabaseCredentials.load(db_block_name)
    s3_credentials = AwsCredentials.load(s3_block_name)
    s3_client_parameters = AwsClientParameters(endpoint_url=s3_endpoint)

    # Figure out start time
    if not full_sync:
        last_modified_date = get_last_run_config("%Y-%m-%d")

    url_list = get_url_list(
        postgres_creds,
        since=last_modified_date if not full_sync else None,
    )

    for i in range(0, len(url_list), batch_size):
        batch = url_list[i : i + batch_size]
        # representation_id, url
        transcript = create_and_upload_transcript_batch.submit(
            batch=batch,
            s3_bucket_name=s3_bucket_name,
            s3_credentials=s3_credentials,
            s3_client_parameters=s3_client_parameters,
        )

        if not transcript.wait().is_failed():
            # representation_id, s3_key, alto_json
            insert_schema_transcript_batch.submit(
                batch=transcript.result(),
                postgres_credentials=postgres_creds,
                wait_for=transcript,
            )

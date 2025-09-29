import os
from datetime import datetime
from typing import Any

from pendulum.datetime import DateTime
from prefect import flow, get_run_logger, task
from prefect.states import Completed, Failed
from prefect.task_runners import ConcurrentTaskRunner
from prefect_meemoo.config.last_run import save_last_run_config
from prefect_sqlalchemy.credentials import DatabaseCredentials
from psycopg2 import DatabaseError, connect, sql
from psycopg2.extras import RealDictCursor
from pathlib import Path

UPDATE_PARTITION_SQL = (Path(__file__).parent / "queries" / "update_partition.sql").read_text()

def get_min_date(format: str = "%Y-%m-%dT%H:%M:%S.%fZ") -> str:
    return datetime.min.strftime(format)

@task(tags=["arc-index-loading"])
def get_partitions(
    db_credentials: DatabaseCredentials, or_ids: list[str] = None, since: str = None
) -> list[dict[str, Any]]:
    """Fetch partitions and counts from the database."""
    with connect(
        user=db_credentials.username,
        password=db_credentials.password.get_secret_value(),
        host=db_credentials.host,
        port=db_credentials.port,
        database=db_credentials.database,
        cursor_factory=RealDictCursor,
    ) as db_conn:
        with db_conn.cursor() as cursor:
            where_clause = "WHERE ie.relation_is_part_of IS null"
            where_clause += " AND org_identifier IN %(or_ids)s" if or_ids else ""
            where_clause += " AND ie.updated_at >= %(since)s" if since else ""
            query = sql.SQL(
                f"""
                SELECT 
                    ie.schema_maintainer as id, 
                    lower(org_identifier) as index,
                    lower(replace(org_identifier, '-','_')) as partition,
                    count(*) as cnt
                FROM graph.intellectual_entity ie 
                JOIN graph.organization o ON ie.schema_maintainer = o.id
                {where_clause}
                GROUP BY 1,2,3 
                ORDER BY 4 ASC
                """
            )
            params = {}
            if since:
                params["since"] = since
            if or_ids:
                params["or_ids"] = tuple(or_ids)
            cursor.execute(query, params if params else None)
            return cursor.fetchall()

@task(tags=["arc-index-loading"])
def create_partition(
    db_credentials: DatabaseCredentials, partition: str, index: str
) -> None:
    """Create a partition if it does not exist."""
    with connect(
        user=db_credentials.username,
        password=db_credentials.password.get_secret_value(),
        host=db_credentials.host,
        port=db_credentials.port,
        database=db_credentials.database,
    ) as db_conn:
        with db_conn.cursor() as cursor:
            create_query = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {db_table} "
                "PARTITION OF graph.index_documents FOR VALUES IN (%(index)s);"
            ).format(db_table=sql.Identifier("graph", partition))
            cursor.execute(create_query, {"index": index})
            db_conn.commit()

@task(tags=["arc-index-loading"])
def truncate_partition(
    db_credentials: DatabaseCredentials, partition: str
) -> None:
    """Truncate a partition (used in full sync)."""
    with connect(
        user=db_credentials.username,
        password=db_credentials.password.get_secret_value(),
        host=db_credentials.host,
        port=db_credentials.port,
        database=db_credentials.database,
    ) as db_conn:
        with db_conn.cursor() as cursor:
            truncate_query = sql.SQL("TRUNCATE {db_table};").format(
                db_table=sql.Identifier("graph", partition)
            )
            cursor.execute(truncate_query)
            db_conn.commit()


@task(tags=["arc-index-loading"])
def populate_index_table(
    db_credentials: DatabaseCredentials,
    row: dict[str, Any],
    since: str,
):
    """Update partition data only (creation/truncation handled in main flow)."""
    logger = get_run_logger()
    partition = row["partition"]
    row = dict(row)
    try:
        with connect(
            user=db_credentials.username,
            password=db_credentials.password.get_secret_value(),
            host=db_credentials.host,
            port=db_credentials.port,
            database=db_credentials.database,
            cursor_factory=RealDictCursor,
        ) as db_conn:
            with db_conn.cursor() as cursor:
                effective_since = since if since is not None else get_min_date()
                logger.info(
                    "Updating %s since %s (%s records).",
                    partition, effective_since, row["cnt"]
                )
                cursor.execute(
                    UPDATE_PARTITION_SQL,
                    {"id": row["id"], "since": effective_since},
                )
                affected = cursor.rowcount
                db_conn.commit()

                logger.info(
                    "Partition %s updated with %s records.",
                    partition, affected
                )

    except (Exception, DatabaseError):
        logger.exception("Error updating partition %s", partition)
        raise

@task(tags=["arc-index-loading"])
def check_if_org_name_changed(
    db_credentials: DatabaseCredentials,
    partition: dict[str, Any],
) -> bool:
    """Check if any of the organization names have changed."""
    logger = get_run_logger()   
    with connect(
        user=db_credentials.username,
        password=db_credentials.password.get_secret_value(),
        host=db_credentials.host,
        port=db_credentials.port,
        database=db_credentials.database,
        cursor_factory=RealDictCursor,
    ) as db_conn:
        with db_conn.cursor() as cursor:
            # check if table exists
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_tables
                    WHERE schemaname = 'graph'
                    AND tablename = %(partition)s
                ) AS table_exists;
                """,
                {"partition": partition["partition"]},
            )
            result = cursor.fetchone()
            if not result["table_exists"]:
                logger.info("Partition %s does not exist yet", partition["partition"])
                return False
            query = sql.SQL(
                """
                    SELECT EXISTS (
                    SELECT 1
                        from {db_table} ind
                    join graph.organization o
                        ON lower(o.org_identifier) = ind."index"
                    WHERE o.org_identifier = %(id)s
                        AND ind.document->'schema_maintainer'->>'schema_name' != o.skos_pref_label
                    LIMIT 1
                    ) AS has_mismatch;
                """
            ).format(db_table=sql.Identifier("graph", partition["partition"]))
            cursor.execute(
                query,
                {"id": partition["id"]},
            )
            result = cursor.fetchone()
            if not result["has_mismatch"]:
                logger.info("No organization name change for %s", partition["id"])
            else:
                logger.warning("Organization name changed for %s", partition["id"])
            return result["has_mismatch"]
            
@flow(
    name="arc_db_load_index_tables_flow",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def arc_db_load_index_tables_flow(
    db_block_name: str,
    last_modified: DateTime = None,
    or_ids: list[str] = None,
    full_sync: bool = False,
):
    logger = get_run_logger()
    postgres_creds = DatabaseCredentials.load(db_block_name)
    partitions = get_partitions.submit(postgres_creds, or_ids, last_modified).result()

    for partition in partitions:

        # --- Check if org name changed ---
        org_name_changed = check_if_org_name_changed.with_options(name=f"Check name change for {partition["id"]}").submit(postgres_creds, partition).result()
        if org_name_changed:
            logger.warning(
                "Organization name changed for %s, dropping partition %s",
                partition["id"], partition["partition"]
            )

        # --- Truncate if full sync or org name changed ---
        if (full_sync and not or_ids) or org_name_changed:
            partition_trunctation = truncate_partition.with_options(name=f"Truncate partition {partition["partition"]}").submit(
                postgres_creds, partition["partition"],
                wait_for=[org_name_changed] if org_name_changed else None,
            )
            logger.info("Truncated partition %s", partition["partition"])

        # --- Create partition ---
        partition_creation = create_partition.with_options(name=f"Create partition {partition["partition"]} if not exists").submit(
            postgres_creds,
            partition["partition"],
            partition["index"],
            wait_for=[partition_trunctation, org_name_changed] if (full_sync and not or_ids) or org_name_changed else None,
        )
        logger.info("Created partition %s", partition["partition"])

        # --- update task ---
        populate_index_table.with_options(name=f"Populate index table {partition["partition"]}").submit(
            db_credentials=postgres_creds,
            row=partition,
            since=last_modified if not full_sync and not org_name_changed else None,
            wait_for=[partition_creation],
        )



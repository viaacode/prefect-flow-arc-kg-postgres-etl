# prefect-flow-arc-kg-postgres-etl

Prefect ETL that syncs the meemoo RDF **knowledge graph** (stored in [TriplyDB](https://triply.cc/)) into a **PostgreSQL** database and an **Elasticsearch** index that power [hetarchief.be](https://hetarchief.be) V3.

The pipeline combines several techniques:

- **SPARQL `CONSTRUCT` queries** run as TriplyDB query jobs / pipelines to build a filtered, relational-shaped view over the graph.
- **TypeScript + [Node.js stream processing](https://nodejs.org/api/stream.html)** to download and load that view into Postgres without holding the full graph in memory.
- **[TriplyDB.js](https://docs.triply.cc/triplydb-js/)** to talk to TriplyDB and **[pg-promise](https://github.com/vitaly-t/pg-promise)** to talk to Postgres.
- **SQL** to create/merge tables and maintain index partitions.
- **[Prefect](https://docs.prefect.io/) flows** to orchestrate view construction, database loading, index-table population, Elasticsearch indexing and delete processing.

---

## Architecture

The work is split into two layers:

1. **Prefect flows** (Python, in [flows/](flows/)) — orchestration. They load credentials from Prefect blocks, run the TypeScript scripts as subprocesses, and run pure-SQL maintenance tasks.
2. **TypeScript scripts** (in [typescript/](typescript/)) — the heavy lifting of constructing the view and streaming it into Postgres.

```
TriplyDB (RDF knowledge graph)
        │
        │  ① 1_kg_view_construct.js  — SPARQL CONSTRUCT pipeline
        ▼
TriplyDB (filtered "view" graph)
        │
        │  ② 2_database_load.js      — stream download + load
        ▼
PostgreSQL  ──③ index tables──▶ Elasticsearch
        │
        └──④ delete processing
```

### Prefect flows

| Flow | File | Responsibility |
| --- | --- | --- |
| `main_flow` | [flows/main_flow.py](flows/main_flow.py) | Orchestrator. Runs each of the flows below (as deployments) in order, wiring `full_sync` / `last_modified` / `or_ids` through and skipping the run if a blocking deployment is already active. |
| `kg_view_flow` | [flows/kg_view_flow.py](flows/kg_view_flow.py) | Runs [`1_kg_view_construct.js`](#typescript-scripts) to (re)build the view graph in TriplyDB. |
| `arc_db_load_flow` | [flows/arc_db_load_flow.py](flows/arc_db_load_flow.py) | Runs [`2_database_load.js`](#typescript-scripts) to load the view into Postgres. For a full sync it waits until `full_sync_hour` before starting. |
| `arc_db_load_index_tables_flow` | [flows/arc_db_load_index_tables_flow.py](flows/arc_db_load_index_tables_flow.py) | Maintains the `graph.index_documents` partitions (one per organisation): creates/truncates/repopulates partitions, and re-populates a partition when an organisation's name changed. |
| `arc_db_delete_flow` | [flows/arc_db_delete_flow.py](flows/arc_db_delete_flow.py) | Processes deletes: removes intellectual entities & fragments flagged `is_deleted`, and drops index partitions for organisations that no longer have records. |

> An external indexing flow (`prefect-flow-arc-indexer`) and an ALTO-to-JSON flow are invoked as separate deployments from `main_flow`; they are not part of this repository.

Full-sync vs. incremental behaviour is driven by `full_sync` and `last_modified`/`SINCE`:
- **Full sync** (`SINCE` unset): target tables are truncated and reloaded (`TRUNCATE+INSERT`).
- **Incremental** (`SINCE` set): only changed records are constructed and merged (`MERGE INTO` / `INSERT … ON CONFLICT`).

### TypeScript scripts

The two flows above delegate their heavy lifting to these scripts:

| Script | Source | Run by | Purpose |
| --- | --- | --- | --- |
| `1_kg_view_construct.js` | [typescript/src/1_kg_view_construct.ts](typescript/src/1_kg_view_construct.ts) | [`kg_view_flow`](#prefect-flows) | Runs the SPARQL `CONSTRUCT` queries as a TriplyDB pipeline, writing a filtered relational view into a single destination graph. Deletes the destination graph first, then runs all queries (optionally scoped to a set of organisation IDs and/or a `SINCE` timestamp for incremental syncs). |
| `2_database_load.js` | [typescript/src/2_database_load.ts](typescript/src/2_database_load.ts) | [`arc_db_load_flow`](#prefect-flows) | Downloads the view graph as gzipped Turtle and processes it as a stream: triples sharing a subject become a **record**, records for the same table become a **batch**, and batches are `COPY`ed into per-table **temp tables**. Temp tables are then merged into their target `graph.*` tables in FK-dependency order, and finally cleaned up. |

The SPARQL `CONSTRUCT` queries live in [typescript/queries/](typescript/queries/) (`av-audio`, `av-video`, `av-complex`, `newspaper`, `iiif`, `organization`, `person`, …). Supporting SQL snippets used during loading are in [typescript/queries/sql/](typescript/queries/sql/).

The record → batch → temp-table streaming logic lives in [typescript/src/stream.ts](typescript/src/stream.ts), [typescript/src/database.ts](typescript/src/database.ts) and [typescript/src/configuration.ts](typescript/src/configuration.ts).

---

## Running the loading scripts directly (Node.js)

You can run the TypeScript loading scripts in isolation, without Prefect, Elasticsearch, or delete processing — useful for local development and debugging.

Requires **Node.js ≥ 18**. From [typescript/](typescript/):

```bash
npm install          # install dependencies
npm run build        # compile TypeScript into lib/

# construct the view graph in TriplyDB
dotenvx run -f .env -- node lib/1_kg_view_construct.js

# stream the view into Postgres
dotenvx run -f .env -- node --inspect lib/2_database_load.js
```

Configuration is read from environment variables (see [typescript/src/configuration.ts](typescript/src/configuration.ts)). Example `.env`:

```bash
# TriplyDB
TRIPLYDB_TOKEN=
TRIPLYDB_OWNER=meemoo
TRIPLYDB_DATASET=knowledge-graph
TRIPLYDB_DESTINATION_DATASET=hetarchief-test
TRIPLYDB_DESTINATION_GRAPH=hetarchief

# PostgreSQL
POSTGRES_USERNAME=hetarchief
POSTGRES_HOST=localhost
POSTGRES_DATABASE=hetarchief
POSTGRES_PASSWORD=password
POSTGRES_PORT=5432
POSTGRES_SSL=False
POSTGRES_POOL_MIN=0
POSTGRES_POOL_MAX=5
POSTGRES_USE_MERGE=True      # MERGE INTO (True) vs INSERT ON CONFLICT (False) for incremental upserts

# Behaviour
BATCH_SIZE=100               # max records inserted per table per query
# SINCE=2024-12-17T01:08:04.851Z  # incremental sync since timestamp; unset = full sync
# OR_IDS=OR-xxxxxxx,OR-yyyyyyy    # restrict the view to these organisation ids
# RECORD_LIMIT=100                # stop early after N records (debugging)
# TABLES=graph."intellectual_entity"  # load only specific tables

# Debug toggles
SKIP_CLEANUP=True            # keep view graph + temp tables around for inspection
LOGGING_LEVEL=DEBUG
DEBUG_MODE=False             # log heap/memory diffs
```

---

## Running via Prefect

Deployments are declared in [deployments.yaml](deployments.yaml) and all target the `q-knowledge-graph` work queue:

| Deployment | Flow |
| --- | --- |
| (main) | `main_flow` |
| `kg-view` | `kg_view_flow` |
| `db-load` | `arc_db_load_flow` |
| `db-load-index-tables` | `arc_db_load_index_tables_flow` |
| `db-delete` | `arc_db_delete_flow` |

The flows load their secrets from Prefect blocks:

- **`triplydb`** — a `TriplyDBCredentials` block for the TriplyDB connection.
- **`local`** (or the configured `db_block_name`) — a `DatabaseCredentials` block for Postgres.
- **`arc-elasticsearch`** — Elasticsearch credentials (used by the external indexer).

### Flow parameters

#### `main_flow` ([flows/main_flow.py](flows/main_flow.py))

| Parameter | Default | Description |
| --- | --- | --- |
| `deployment_kg_view_flow` | — | `DeploymentModel` for the view-construction sub-flow |
| `deployment_arc_db_load_flow` | — | `DeploymentModel` for the Postgres-load sub-flow |
| `deployment_arc_db_load_index_tables_flow` | — | `DeploymentModel` for the index-table sub-flow |
| `deployment_arc_db_delete_flow` | — | `DeploymentModel` for the delete sub-flow |
| `deployment_arc_alto_to_json_flow` | — | `DeploymentModel` for the (external) ALTO-to-JSON flow |
| `deployment_arc_indexer_flow` | — | `DeploymentModel` for the (external) Elasticsearch indexer |
| `last_modified` | `None` | Incremental `SINCE` timestamp, propagated to sub-flows |
| `or_ids` | `None` | Restrict the run to these organisation ids |
| `full_sync` | `False` | Force a full reload across the sub-flows |

#### `kg_view_flow` ([flows/kg_view_flow.py](flows/kg_view_flow.py))

| Parameter | Default | Description |
| --- | --- | --- |
| `triplydb_block_name` | `"triplydb"` | TriplyDB credentials block name |
| `triplydb_owner` | `"meemoo"` | TriplyDB account |
| `triplydb_dataset` | `"knowledge-graph"` | Source dataset |
| `triplydb_destination_dataset` | `"hetarchief"` | Dataset holding the view |
| `triplydb_destination_graph` | `"hetarchief"` | Graph holding the view |
| `base_path` | `"/opt/prefect/typescript/"` | Location of the JS files |
| `prefix_id_base` | `"https://data.hetarchief.be/id/entity/"` | Base IRI for entity ids |
| `script_path` | `"lib/"` | Compiled JS subfolder |
| `last_modified` | `None` | Incremental `SINCE` timestamp |
| `full_sync` | `False` | Full reload vs incremental |
| `or_ids` | `None` | Restrict the view to these organisation ids |
| `logging_level` | Prefect's level | JS log level |

#### `arc_db_load_flow` ([flows/arc_db_load_flow.py](flows/arc_db_load_flow.py))

| Parameter | Default | Description |
| --- | --- | --- |
| `triplydb_block_name` | `"triplydb"` | TriplyDB credentials block name |
| `triplydb_owner` | `"meemoo"` | TriplyDB account |
| `triplydb_dataset` | `"knowledge-graph"` | Source dataset |
| `triplydb_destination_dataset` | `"hetarchief"` | Dataset holding the view |
| `triplydb_destination_graph` | `"hetarchief"` | Graph holding the view |
| `base_path` | `"/opt/prefect/typescript/"` | Location of the JS files |
| `script_path` | `"lib/"` | Compiled JS subfolder |
| `db_block_name` | `"local"` | Postgres credentials block |
| `db_ssl` | `True` | Use SSL for Postgres |
| `db_pool_min` / `db_pool_max` | `0` / `5` | Connection pool bounds |
| `db_loading_batch_size` | `100` | Records inserted per query |
| `record_limit` | `None` | Cap on records loaded (debug) |
| `last_modified` | `None` | Incremental `SINCE` timestamp |
| `or_ids` | `None` | Restrict to organisation ids |
| `full_sync` | `False` | Full reload vs incremental |
| `sync_tables` | `None` | Restrict loading to specific tables |
| `full_sync_hour` | `0` | Hour (Europe/Brussels) at which a full sync is allowed to start |
| `debug_mode` | `False` | Extra memory logging |
| `logging_level` | Prefect's level | JS log level |

#### `arc_db_load_index_tables_flow` ([flows/arc_db_load_index_tables_flow.py](flows/arc_db_load_index_tables_flow.py))

| Parameter | Default | Description |
| --- | --- | --- |
| `db_block_name` | — | Postgres credentials block |
| `last_modified` | `None` | Incremental `SINCE` timestamp |
| `or_ids` | `None` | Restrict to organisation ids |
| `full_sync` | `False` | Truncate & repopulate all partitions |

#### `arc_db_delete_flow` ([flows/arc_db_delete_flow.py](flows/arc_db_delete_flow.py))

| Parameter | Default | Description |
| --- | --- | --- |
| `db_block_name` | — | Postgres credentials block |

---

## Build & deployment

The container image is built from [Dockerfile](Dockerfile), based on meemoo's `prefect-triplyetl` image. It installs the Python requirements ([requirements.txt](requirements.txt)), copies `flows/` and `typescript/` into `/opt/prefect`, and runs `npm ci && npm run build` to compile the TypeScript. CI is defined in [.openshift/Jenkinsfile](.openshift/Jenkinsfile).

Python dependencies of note (see [requirements.txt](requirements.txt)): `prefect==2.20.16`, `prefect-meemoo[triplydb,config]`, `prefect-sqlalchemy`, `psycopg2-binary`.

---

## Repository layout

```
flows/                     Prefect flows (Python)
  main_flow.py             orchestrator
  kg_view_flow.py          view construction
  arc_db_load_flow.py      Postgres loading
  arc_db_load_index_tables_flow.py   index partition maintenance
  arc_db_delete_flow.py    delete processing
  queries/update_partition.sql
typescript/
  src/                     TypeScript sources
    1_kg_view_construct.ts view construction entrypoint
    2_database_load.ts     streaming loader entrypoint
    stream.ts, database.ts, configuration.ts, helpers.ts, ...
  queries/                 SPARQL CONSTRUCT queries (+ sql/ helpers)
  package.json, tsconfig.json, Dockerfile, docker-compose.yml
Dockerfile                 image build
deployments.yaml           Prefect deployment definitions
requirements.txt           Python dependencies
infra_block.py             Prefect infrastructure block
```

---

## License

See [LICENSE](LICENSE).

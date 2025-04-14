# Prefect Flow: arc-kg-postgres-etl

This prefect flow syncs the meemoo RDF knowledge graph stored in TriplyDB to a Postgres database used for hetarchief.be V3.

It combines the following techniques:
- SPARQL CONSTRUCT queries and TriplyDB query pipelines/jobs
- Typescript and [NodeJS Stream processing](https://nodejs.org/api/stream.html)
- The [TriplyDB.js](https://docs.triply.cc/triplydb-js/) and [pg-promise](https://github.com/vitaly-t/pg-promise) libraries to communicate with TriplyDB and Postgres, repectively.
- SQL queries to manipulate the postgres database such as creating tables or inserting data.
- Prefect flow to orchestrate the loading of tables, the indexing of Elasticsearch and processing deletes.

## High-level walkthrough

The synchronization proces consist of 
- `flows/main_flow.py`: a Prefect Flow (Python) and 
- `typescript/index.js`: a data stream processing and loading script (Typescript/NodeJS)
  
The latter is run by the former. Here's a quick overview on both components

### 1. flows/main_flow.py

- The flow first runs the data stream processing and loading script to fill or update the Postgres database with metadata.
- Then, it runs the indexer to index documents from Postgres into Elasticsearch
- Finally, it processes the deletes in case of an incremental run.

### 2. typescript/index.js

- The script first runs a couple of commands on the TriplyDB instance
  - a. copy all graphs in the Knowledge Graph to a single graph to enhance performance
  - b. create a new graph containing a filtered view over the Knowledge Graph using a number of SPARQL CONSTRUCT queries
- Then, it downloads the view graph and processes it as a stream of triples
  - triples with the same subject are turned into a record
  - records belonging to the same table are grouped into a batch
  - batches are inserted into temporary copy of the target table as soon as their maximum size is reached. 
- Next, all temporary tables are merge into their target table
- Finally, all created graphs and temporary tables are deleted

![alt text](diagram.png)

## How to run?

### Running the loading scripts using nodejs

You can run the database loading script in isolation without executing the indexer and performing deletes. 

First, make sure you have NodeJS > 18 installed.
Then, from the `./typescript` folder, run

```bash
npm install # installs dependencies
npm run build # builds the typescript sources
dotenvx run -f .env -- node --inspect lib/index.js # runs using env file
```

The `.env` file contains all the necessary configuration, such as connection details to postgres and TriplyDB.

Example .env file:
```bash
TRIPLYDB_TOKEN=
TRIPLYDB_OWNER=meemoo
TRIPLYDB_DATASET=knowledge-graph
TRIPLYDB_DESTINATION_DATASET=hetarchief-test
TRIPLYDB_DESTINATION_GRAPH=hetarchief
POSTGRES_USERNAME=hetarchief
POSTGRES_HOST=localhost
POSTGRES_DATABASE=hetarchief
POSTGRES_PASSWORD=password
POSTGRES_PORT=5555
SKIP_VIEW=True # Skip building the view graph
SKIP_CLEANUP=True # Do not remove view graph and temp tables when done (for debugging)
SKIP_SQUASH=True # Do not squash the graphs before constructing the view graph
LOGGING_LEVEL=DEBUG
BATCH_SIZE=100 # The maximum number of records that are inserted per table in a single query
DEBUG_MODE=False # Print memory information in logs
#SINCE=2024-12-17T01:08:04.851Z # Run an incremental update since 2024-12-17T01:08:04.851Z. If SINCE is not set, a full sync is run.
#RECORD_LIMIT=100 # Cut the process off early
```

### Running the Prefect Flow

The Prefect Flow requires setting the following parameters:

- `triplydb_block_name`: name of the TriplyDB credentials block (default:`"triplydb"`)
- `triplydb_owner`: name of the account in  TriplyDB (default:`"meemoo"`)
- `triplydb_dataset`: name of the dataset in TriplyDB (default:`"knowledge-graph"`)
- `triplydb_destination_dataset`: name of the dataset to store the view in (default:`"hetarchief"`)
- `triplydb_destination_graph`: name of the graph to store the view in (default:`"hetarchief"`)
- `base_path`: folder where javascript files are stored (default:`"/opt/prefect/typescript/"`)
- `script_path`: folder of the javascript script (default:`"lib/"`)
- `skip_squash`: skip copying all graphs to a single graph (default:`False`)
- `skip_view`: skip creating a view graph (default:`False`)
- `skip_cleanup`: skip cleanup of all graphs (default:`False`)
- `es_block_name`: name of the elasticsearch block (default:`"arc-elasticsearch"`)
- `es_chunk_size`: elasticsearch streaming chunk size (default:`500`)
- `es_request_timeout`: number of seconds before elasticsearch request times out (default:`30`)
- `es_max_retries`: number of times a failed document should be retried (default:`10`)
- `es_retry_on_timeout`: retry when a document times out (default:`True`)
- `db_indexing_batch_size`: size of the database cursor to read documents with (default:`500`)
- `db_block_name`: name of the database block (default:`"local"`)
- `db_index_table`: table whene the index documents are stored (default:`"graph._index_intellectual_entity"`)
- `db_ssl`: enable SSL connection for database (default:`True`)
- `db_pool_min`: minimum connections in pool (default:`0`)
- `db_pool_max`: maximum connections in pool (default:`5`)
- `db_loading_batch_size`: number of records that are inserted in a single query (default:`100`)
- `record_limit`: limit the number of records that are being loaded (default:`None`)
- `full_sync`: sync everything (default:`False`)
- `debug_mode`: print extra logging about memory consumption (default:`False`)
- `logging_level`: set the logging level of Javascript (default:`os.environ.get("PREFECT_LOGGING_LEVEL")` = same as prefect)
- `flow_name_indexer`: name of the indexing flow (default:`"prefect-flow-arc-indexer"`)
- `deployment_name_indexer`: name of the indexing flow deployment (default:`(default: "prefect-flow-arc-indexer-int")`)



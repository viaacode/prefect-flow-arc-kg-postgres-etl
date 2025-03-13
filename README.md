# Prefect Flow: arc-kg-postgres-etl

This prefect flow syncs the meemoo RDF knowledge graph stored in TriplyDB to a Postgres database used for hetarchief.be V3.

It combines the following techniques:
- SPARQL CONSTRUCT queries and TriplyDB query pipelines/jobs
- Typescript and [NodeJS Stream processing](https://nodejs.org/api/stream.html)
- The [TriplyDB.js](https://docs.triply.cc/triplydb-js/) and [pg-promise](https://github.com/vitaly-t/pg-promise) libraries to communicate with TriplyDB and Postgres, repectively.
- SQL queries to manipulate the postgres database such as creating tables or inserting data.
- Prefect flow to orchestrate the loading of tables, the indexing of Elasticsearch and processing deletes.

## High-level walkthrough



```mermaid
C4Component
title Component diagram for Internet Banking System - API Application

Container(flow, "Prefect Flow ", "Python and Typescript", "Provides all the internet banking functionality to customers via their web browser.") {
    Component(task-ts, "Prefect Task: run_javascript", "Prefect Task", "Runs a Javascript script as subprocess") {
        Component(task-deletes, "index.js", "NodeJS", "Create records from the knowledge graph and insert them into Postgres") {
            Component(step0, "Squash Graphs", "NodeJS", "") 
            Component(step1, "Construct views", "NodeJS", "") 
            Component(step2, "Load temporary tables", "NodeJS", "") 
            Component(step3, "Merge tables", "NodeJS", "") 
            Component(step4, "Cleanup", "NodeJS", "") 
            
            Rel(step0, step1, "")
            Rel(step1, step2, "")
            Rel(step2, step3, "")
            Rel(step3, step4, "")
        }
    }
    Component(task-deletes, "Prefect Task: delete_records_from_db", "Prefect Task", "Provides customers with a summary of their bank accounts")
    Component(task-indexer, "Prefect Task: run_deployment_task", "Prefect Task", "Provides functionality related to singing in, changing passwords, etc.")

    Rel(task-ts, task-indexer, "wait_for")
    Rel(task-indexer, task-deletes, "wait_for")
}
```




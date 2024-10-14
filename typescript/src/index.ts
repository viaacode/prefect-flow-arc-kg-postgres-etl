import { Quad } from 'rdf-js'
import pg from 'pg'
import { from } from 'pg-copy-streams'
import { fromRdf } from 'rdf-literal'
import { stringify } from 'csv-stringify'
import { stringify as stringifySync } from 'csv-stringify/sync'
import { pipeline } from 'node:stream/promises'
import { parse as parseDuration, toSeconds } from "iso8601-duration"
import App from '@triply/triplydb'
import { Account } from '@triply/triplydb/Account.js'
import { AddQueryOptions } from '@triply/triplydb/commonAccountFunctions.js'
import Graph from '@triply/triplydb/Graph.js'
import { readdir, readFile } from 'fs/promises'
import { join, extname, parse } from 'path'
import Dataset from '@triply/triplydb/Dataset.js'
import {
    BATCH_SIZE, dbConfig, RECORD_LIMIT, QUERY_PATH, NAMESPACE,
    XSD_DURATION, ACCOUNT, DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, TOKEN,
    SINCE,
    GRAPH_BASE,
    SKIP_SQUASH,
    TABLE_PRED,
    SKIP_VIEW
} from './configuration.js'
import { logInfo, logError, logDebug, getErrorMessage, isValidDate } from './util.js'
import { DepGraph } from 'dependency-graph'

class TableInfo {
    private _name: string
    private _schema: string

    constructor(schema: string, name?: string) {
        if (!name) {
            const parts = schema.split('.')
            name = parts[1]
            schema = parts[0]
        }

        this._schema = schema
        this._name = name
    }

    public get schema() {
        return this._schema
    }

    public get name() {
        return this._name
    }

    public toString = (): string => {
        return `${this._schema}."${this._name}"`
    }
}

type ColumnInfo = { name: string, datatype: string }
type TableNode = { tempTable: TableInfo, columns: ColumnInfo[], dependencies: TableInfo[], primaryKeys: string[] }
type Destination = { dataset: Dataset, graph: string }

const tableIndex = new DepGraph<TableNode>()

// PostgreSQL connection pool
const pool = new pg.Pool(dbConfig)

let unprocessedBatches = 0
let batchCount = 0

async function addQuery(account: Account, queryName: string, params: AddQueryOptions) {
    try {
        const query = await account.getQuery(queryName)
        await query.delete()
        logInfo(`Query ${queryName} deleted.\n`)
    } catch (err) {
        logInfo(`Query ${queryName} does not exist.\n`)
    }
    return account.addQuery(queryName, params)
}

async function addJobQueries(account: Account, source: Dataset) {
    const files = (await readdir(QUERY_PATH))
        // Only use sparql files
        .filter(f => extname(f) === '.sparql')

    const queries = []
    for (const file of files) {
        const filePath = join(QUERY_PATH, file)
        const queryString = await readFile(filePath, 'utf8')
        const queryName = parse(filePath).name

        const params: AddQueryOptions = {
            dataset: source,
            queryString,
            serviceType: 'speedy',
            output: 'response',
            variables: [
                {
                    name: 'since',
                    required: false,
                    termType: 'Literal',
                    datatype: 'http://www.w3.org/2001/XMLSchema#dateTime',
                }
            ]
        }

        const query = await addQuery(account, queryName, params)
        queries.push(query)
    }
    return queries
}

// Helper function to create a table dynamically based on the columns
async function createTempTable(tableInfo: TableInfo): Promise<TableInfo> {
    // Construct temp table name
    const { schema, name } = tableInfo
    const tempTableInfo = new TableInfo(schema, `temp_${name}`)

    logInfo(`Creating temp table ${tempTableInfo} from ${tableInfo} if not exists.`)
    const query = `
        DROP TABLE IF EXISTS ${tempTableInfo};
        CREATE TABLE ${tempTableInfo} (LIKE ${tableInfo} INCLUDING ALL EXCLUDING CONSTRAINTS);
    `
    const client = await pool.connect()
    try {
        await client.query(query)
        return tempTableInfo
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error creating table ${tempTableInfo}:`, msg)
        throw err
    }
    finally {
        client.release()
    }
}

async function dropTable(tableInfo: TableInfo) {
    logInfo(`Dropping table ${tableInfo} if exists.`)
    const query = `
        DROP TABLE IF EXISTS ${tableInfo};
    `
    const client = await pool.connect()
    try {
        await client.query(query)
        return tableInfo
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error dropping table ${tableInfo}:`, msg)
        throw err
    }
    finally {
        client.release()
    }
}

// Helper function to retrieve column names for a specific table
async function getTableColumns(tableInfo: TableInfo): Promise<ColumnInfo[]> {
    const client = await pool.connect()
    const query = `
        SELECT column_name AS name, data_type AS datatype
        FROM information_schema.columns
        WHERE table_name = $1 AND table_schema = $2
    `
    logDebug(query)
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error retrieving columns for table ${tableInfo}:`, msg)
        throw err
    } finally {
        client.release()
    }
}

// Helper function to retrieve primary keys for a specific table
async function getDependentTables(tableInfo: TableInfo): Promise<TableInfo[]> {
    const client = await pool.connect()
    const query = `
        SELECT DISTINCT
            ccu.table_schema AS schema,
            ccu.table_name AS name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema=$2
            AND tc.table_name=$1;
    `
    logDebug(query)
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error retrieving dependent tables for table ${tableInfo}:`, msg)
        throw err
    } finally {
        client.release()
    }
}

// Helper function to retrieve primary keys for a specific table
async function getTablePrimaryKeys(tableInfo: TableInfo): Promise<string[]> {
    const client = await pool.connect()
    const query = `
        SELECT COLUMN_NAME from information_schema.key_column_usage 
        WHERE table_name = $1 AND table_schema = $2 AND constraint_name LIKE '%pkey'
    `
    logDebug(query)
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows.map((row: { column_name: string }) => row.column_name)
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error retrieving columns for table ${tableInfo}:`, msg)
        throw err
    } finally {
        client.release()
    }
}

async function createTableNode(tableName: string): Promise<TableNode> {
    const tableInfo = new TableInfo(tableName)
    const tableNode = {
        tempTable: await createTempTable(tableInfo),
        // Get the actual columns from the database
        columns: await getTableColumns(tableInfo),
        // Get dependent tables from the database
        dependencies: await getDependentTables(tableInfo),
        // Get primary keys
        primaryKeys: await getTablePrimaryKeys(tableInfo)
    }
    tableIndex.addNode(tableName, tableNode)
    return tableNode
}


// Helper function to delete a batch of records
async function processDeletes(tableInfo: TableInfo) {
    const client = await pool.connect()
    const query = `
        DELETE ${tableInfo}
        FROM ${tableInfo} x
        INNER JOIN graph."mh_fragment_identifier" y ON x.id = y.intellectual_entity_id
        WHERE y.is_deleted;
    `
    try {
        await client.query('BEGIN')
        const result = await client.query(query)
        await client.query('COMMIT')
        logInfo(`Deleted ${result} records from table ${tableInfo}`)
    } catch (err) {
        await client.query('ROLLBACK')
        logError(`Error during batch delete for table ${tableInfo}:`, err)
    } finally {
        client.release()
    }
}

async function batchInsertUsingCopy(tableName: string, batch: Array<Record<string, string>>) {
    if (!batch.length) return

    unprocessedBatches++

    // Create temp table if not exists
    const { columns, tempTable } = tableIndex.hasNode(tableName) ? tableIndex.getNodeData(tableName) : await createTableNode(tableName)

    logInfo(`Start batch insert using COPY for ${tableName} using ${tempTable}`)

    // Get the actual columns from the database
    const client = await pool.connect()
    const columnList = columns.map(c => c.name).join(',')
    const copyQuery = `COPY ${tempTable} (${columnList}) FROM STDIN WITH (FORMAT csv)`

    logDebug(copyQuery)

    try {
        await client.query('BEGIN')
        const ingestStream = client.query(from(copyQuery))

        // Initialize the stringifier
        const sourceStream = stringify({
            delimiter: ",",
            cast: {
                date: (value) => {
                    return value.toISOString()
                },
            },
        })

        // Convert batch to CSV format
        for (const record of batch) {
            const values = columns.map(col => {
                // Make sure value exists and that dates are valid dates
                if (!record[col.name] || (col.datatype === 'date' && !isValidDate(record[col.name])))
                    return null

                return record[col.name]
            })
            sourceStream.write(values)
        }
        sourceStream.end()
        await pipeline(sourceStream, ingestStream)
        await client.query('COMMIT')
        batchCount++
        logInfo(`Batch #${batchCount} for ${tableName} inserted!`)
    } catch (err) {
        await client.query('ROLLBACK')
        //TODO: fix error caused by logging
        const msg = getErrorMessage(err)
        logError(`Error during bulk insert for table ${tableName}:`, msg)
        const result = stringifySync(
            batch.map(record => columns.map(col => record[col.name] || null)),
            {
                cast: {
                    date: (value) => {
                        return value.toISOString()
                    },
                }
            }
        )
        logError(`Erroreous batch:`, result)
        throw err
    } finally {
        client.release()
        unprocessedBatches--
    }
}


// Process each record and add it to the appropriate batch
async function processRecord(
    subject: string,
    record: Record<string, string>,
    tableName: string,
    batches: { [tableName: string]: Array<Record<string, string>> }
) {
    logDebug(`Process record for ${subject}: ${JSON.stringify(record)}`)

    if (!record || !tableName || !subject) return

    if (!batches[tableName]) {
        batches[tableName] = []
    }

    batches[tableName].push(record)

    if (batches[tableName].length >= BATCH_SIZE) {
        logDebug(`Maximum batch size reached for ${tableName}; processing.`)
        const batch = batches[tableName]
        await batchInsertUsingCopy(tableName, batch)
        batches[tableName] = []
    }
}

async function upsertTable(tableInfo: TableInfo, tableNode: TableNode, truncate: boolean = true) {
    const client = await pool.connect()

    // Get the actual columns from the database
    const { columns, primaryKeys, tempTable } = tableNode
    const columnList = columns.map(c => `${c.name} = EXCLUDED.${c.name}`).join(',')

    // Build query
    const query = `
        INSERT INTO ${tableInfo}
        SELECT * FROM ${tempTable}
        ON CONFLICT (${primaryKeys.join(',')}) DO UPDATE
        SET ${columnList};
        `
    const truncateQuery = `TRUNCATE ${tableInfo} CASCADE`
    logError(query)
    try {
        await client.query('BEGIN')
        // Truncate table first if desired
        if (truncate) {
            await client.query(truncateQuery)
        }
        await client.query(query)
        await client.query('COMMIT')
        logInfo(`Records for table ${tableInfo} upserted!`)
    } catch (err) {
        await client.query('ROLLBACK')
        const msg = getErrorMessage(err)
        logError(`Error during upsert from '${tempTable}' to '${tableInfo}':`, msg)
        throw err
    } finally {
        client.release()
    }

}


// Main function to parse and process the gzipped TriG file from a URL
async function processGraph(graph: Graph) {
    const quadStream = await graph.toStream('rdf-js')
    return new Promise<void>((resolve, reject) => {

        let recordCount = 0

        const batches: { [tableName: string]: Record<string, string>[] } = {}
        let currentRecord: Record<string, string> | null = null
        let currentSubject: string | null = null
        let currentTableName: string | null = null

        quadStream
            .on('data', async (quad: Quad) => {
                const subject = quad.subject.value
                const predicate = quad.predicate.value
                let object = quad.object.value
                // Convert literal to primitive
                if (quad.object.termType === "Literal") {
                    // Convert duration to seconds first
                    object = quad.object.datatype.value === XSD_DURATION ? toSeconds(parseDuration(object)) : fromRdf(quad.object)
                }

                // If the subject changes, process the current record
                if (subject !== currentSubject) {
                    if (currentSubject !== null && currentRecord && currentTableName !== null) {
                        try {
                            quadStream.pause()
                            await processRecord(currentSubject, currentRecord, currentTableName, batches)
                            logDebug(`Record ${recordCount} (${currentSubject}) processed`)
                        } finally {
                            quadStream.resume()
                        }
                    }
                    recordCount++

                    if (RECORD_LIMIT && recordCount > RECORD_LIMIT) {
                        return quadStream.destroy()
                    }

                    currentSubject = subject
                    currentTableName = null
                    currentRecord = {}
                }

                // Check for the record type
                if (predicate === TABLE_PRED) {
                    currentTableName = object
                }
                // Handle predicates within the known namespace
                else if (predicate.startsWith(NAMESPACE)) {
                    const columnName = predicate.replace(NAMESPACE, '')
                    // TODO: handle multiple values
                    currentRecord![columnName] = object
                }
            })
            .on('end', async () => {
                // Process the last record
                if (currentSubject && currentRecord && currentTableName) {
                    await processRecord(currentSubject, currentRecord, currentTableName, batches)
                }

                // Insert any remaining batches
                for (const tableName in batches) {
                    if (batches[tableName].length) {
                        await batchInsertUsingCopy(tableName, batches[tableName])
                    }
                }

                logInfo(`Processing completed: ${recordCount} records (${batchCount}/${Math.ceil(recordCount / BATCH_SIZE) + tableIndex.size()} batches).`)
                resolve()
            })
            .on('error', (err: Error) => {
                logError('Error during parsing or processing:', err)
                reject(err)
            })
    })
}

// Main execution function
async function main() {
    logInfo(`Starting sync from ${DATASET} to ${DESTINATION_DATASET} (${DESTINATION_GRAPH})`)
    const triply = App.get({ token: TOKEN })

    const account = await triply.getAccount(ACCOUNT)
    // TODO: create dataset if not exists
    let dataset = await account.getDataset(DATASET)
    const destination: Destination = {
        dataset: await account.getDataset(DESTINATION_DATASET),
        graph: GRAPH_BASE + DESTINATION_GRAPH
    }

    logInfo(`--- Syncing ${DATASET} to graph ${destination.graph} of ${DESTINATION_DATASET} ---`)
    if (!SKIP_VIEW) {
        if (!SKIP_SQUASH) {
            logInfo('--- Step 0: Squash graphs ---')
            console.time('Squash graphs')
            const graphName = GRAPH_BASE + DATASET

            await destination.dataset.clear("graphs")
            logInfo(`Cleared graphs of dataset ${DATASET}.`)

            const params: AddQueryOptions = {
                dataset,
                queryString: 'CONSTRUCT WHERE { ?s ?p ?o }',
                serviceType: 'speedy',
                output: 'response',
            }
            const query = await addQuery(account, 'squash-graphs', params)
            logInfo(`Starting pipeline for ${query.slug}.`)
            await query.runPipeline({
                destination: {
                    dataset: destination.dataset,
                    graph: graphName
                }
            })
            logInfo(`Pipeline completed.`)
            dataset = destination.dataset
            console.timeEnd('Squash graphs')
        } else {
            logInfo('--- Skipping squash graphs ---')
        }

        logInfo('--- Step 1: Construct view ---')
        console.time('Construct view')

        const queries = await addJobQueries(account, dataset)

        logInfo(`Starting pipelines for ${queries.map(q => q.slug)}.`)

        await account.runPipeline({
            destination,
            onProgress: progress => logInfo(`Pipeline ${Math.round(progress.progress * 100)}% complete.`),
            queries: queries.map(q => ({
                query: q,
                // TODO report bug on 'less than one properry`
                ...SINCE ? {
                    variables: {
                        since: SINCE
                    }
                } : {}
            })),
        })
        logInfo(`Pipelines completed.`)
        console.timeEnd('Construct view')
    } else {
        logInfo('--- Skipping view construction ---')
    }

    // Parse and process the gzipped TriG file from the URL
    logInfo('--- Step 2: load temporary tables and delete records --')
    console.time('Load and delete records')

    // Get destination graph and process
    const graph = await destination.dataset.getGraph(destination.graph)

    await processGraph(graph)
    console.timeEnd('Load and delete records')

    logInfo('--- Step 3: upsert tables --')
    console.time('Upsert tables')

    // Resolve dependencies to table graph
    tableIndex.entryNodes().forEach(tableName => {
        const node = tableIndex.getNodeData(tableName)
        node.dependencies.forEach(dependency => {
            tableIndex.addDependency(tableName, `${dependency.schema}.${dependency.name}`)
        })
    })


    // Upsert tables in the right order
    for (const tableName of tableIndex.overallOrder()) {
        const tableNode = tableIndex.getNodeData(tableName)

        const tableInfo = new TableInfo(tableName)

        // upsert records from temp table into table; truncate tables if full sync
        await upsertTable(tableInfo, tableNode, !SINCE)
        // drop temp table when done
        await dropTable(tableNode.tempTable)
    }
    console.timeEnd('Upsert tables')

    if (SINCE) { 
        logInfo('--- Step 4: Perform deletes --')
        const tableInfo = new TableInfo('graph','intellectual_entity')
        await processDeletes(tableInfo)
    } else {
        logInfo('--- Skipping deletes because full sync ---')
    }

    logInfo('--- Step 5: Graph cleanup --')
    console.time('Graph cleanup')
    await graph.delete()
    console.timeEnd('Graph cleanup')

    logInfo('--- Sync done. --')
}

main().catch(err => {
    const msg = getErrorMessage(err)
    logError(msg)
    process.exit(1)
}).finally(() => {
    logDebug(`Unprocessed batches: ${unprocessedBatches}`)
    pool.end()
})

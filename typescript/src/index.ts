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
    BATCH_SIZE, dbConfig, RECORD_LIMIT, QUERY_PATH, tables, NAMESPACE,
    XSD_DURATION, ACCOUNT, DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, TOKEN,
    SINCE,
    GRAPH_BASE,
    SQUASH_GRAPHS,
    TABLE_PRED
} from './configuration.js'
import { logInfo, logError, logDebug, getErrorMessage, isValidDate } from './util.js'


type TableInfo = { name: string, schema: string }
type ColumnInfo = { name: string, datatype: string }
type Destination = { dataset: Dataset, graph: string }

const columnCache: { [tableName: string]: ColumnInfo[] } = {}

// PostgreSQL connection pool
const pool = new pg.Pool(dbConfig)

function getTempTableName(tableName: string) {
    const tableInfo = parseTableName(tableName)
    return `${tableInfo.schema}.temp_${tableInfo.name}`
}

function parseTableName(tableName: string): TableInfo {
    const parts = tableName.split('.')
    return { name: parts[1], schema: parts[0] }
}

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
                    termType: 'Literal',
                    datatype: 'http://www.w3.org/2001/XMLSchema#dateTime'
                }
            ]
        }

        const query = await addQuery(account, queryName, params)
        queries.push(query)
    }
    return queries
}

// Helper function to create a table dynamically based on the columns
async function createTempTable(tableName: string) {
    const tempTableName = getTempTableName(tableName)
    logInfo(`Creating temp table ${tempTableName} from ${tableName} if not exists.`)
    const query = `
        CREATE TABLE IF NOT EXISTS ${tempTableName} (LIKE ${tableName} INCLUDING ALL EXCLUDING CONSTRAINTS);
    `
    const client = await pool.connect()
    try {
        await client.query(query)
        return tempTableName
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error creating table ${tempTableName}:`, msg)
        throw err
    }
    finally {
        client.release()
    }
}

async function dropTable(tableName: string) {
    logInfo(`Dropping table ${tableName} if exists.`)
    const query = `
        DROP TABLE IF EXISTS ${tableName};
    `
    const client = await pool.connect()
    try {
        await client.query(query)
        return tableName
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error dropping table ${tableName}:`, msg)
        throw err
    }
    finally {
        client.release()
    }
}

async function getTableColumnsWithCache(tableName: string): Promise<ColumnInfo[]> {
    if (columnCache[tableName]) {
        //console.log(`Returning ${columnCache[tableName].length} columns for ${tableName} from cache.`)
        return columnCache[tableName]
    }
    const columns = await getTableColumns(tableName)
    //console.log(`Returning ${columns} columns for ${tableName}.`)
    columnCache[tableName] = columns
    return columns
}

// Helper function to retrieve column names for a specific table
async function getTableColumns(tableName: string): Promise<ColumnInfo[]> {
    const client = await pool.connect()
    const query = `
        SELECT column_name AS name, data_type AS datatype
        FROM information_schema.columns
        WHERE table_name = $1 AND table_schema = $2
    `
    logDebug(query)
    try {
        const { name, schema } = parseTableName(tableName)
        const result = await client.query(query, [name, schema])
        return result.rows//.map((row: { column_name: string, data_type: string }) => {row.column_name})
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error retrieving columns for table ${tableName}:`, msg)
        throw err
    } finally {
        client.release()
    }
}

// Helper function to retrieve primary keys for a specific table
async function getTablePrimaryKeys(tableName: string): Promise<string[]> {
    const client = await pool.connect()
    const query = `
        SELECT COLUMN_NAME from information_schema.key_column_usage 
        WHERE table_name = $1 AND table_schema = $2 AND constraint_name LIKE '%pkey'
    `
    logDebug(query)
    try {
        const { name, schema } = parseTableName(tableName)
        const result = await client.query(query, [name, schema])
        return result.rows.map((row: { column_name: string }) => row.column_name)
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error retrieving columns for table ${tableName}:`, msg)
        throw err
    } finally {
        client.release()
    }
}


// Helper function to delete a batch of records based on the 'subject' column
// async function deleteBatch(tableName: string, ids: string[]) {
//     if (!ids.length) return

//     const client = await pool.connect()
//     const query = `
//         DELETE FROM "${tableName}"
//         WHERE id = ANY($1::text[]);
//     `

//     try {
//         await client.query('BEGIN')
//         await client.query(query, [ids])
//         await client.query('COMMIT')
//         console.log(`Deleted ${ids.length} records from table ${tableName}`)
//     } catch (err) {
//         await client.query('ROLLBACK')
//         logError(`Error during batch delete for table ${tableName}:`, err)
//     } finally {
//         client.release()
//     }
// }


async function batchInsertUsingCopy(tableName: string, batch: Array<Record<string, string>>) {
    if (!batch.length) return

    // Create table if not exists
    const tempTableName = await createTempTable(tableName)

    logInfo(`Start batch insert using COPY for ${tableName} using ${tempTableName}`)

    // Get the actual columns from the database
    const columns = await getTableColumnsWithCache(tableName)

    const client = await pool.connect()
    const columnList = columns.map(c => c.name).join(',')
    const { schema, name } = parseTableName(tempTableName)
    const copyQuery = `COPY ${schema}."${name}" (${columnList}) FROM STDIN WITH (FORMAT csv)`

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
        logInfo(`Batch for ${tableName} inserted!`)
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
        logError(result)
        throw err
    } finally {
        client.release()
    }
}


// Process each record and add it to the appropriate batch
async function processRecord(
    subject: string,
    record: Record<string, string>,
    tableName: string,
    batches: { [tableName: string]: Array<Record<string, string>> }
) {
    //console.log(`Process record for ${currentSubject}: ${JSON.stringify(currentRecord)}`)

    if (!record || !tableName || !subject) return

    if (!batches[tableName]) {
        batches[tableName] = []
    }

    batches[tableName].push(record)

    if (batches[tableName].length >= BATCH_SIZE) {
        console.log(`Maximum batch size reached for ${tableName}; processing.`)
        const batch = batches[tableName]
        batchInsertUsingCopy(tableName, batch)
        batches[tableName] = []
    }
}

async function upsertTable(tempTableName: string, tableName: string, truncate: boolean = true) {
    const client = await pool.connect()

    // Get the actual columns from the database
    const columns = await getTableColumnsWithCache(tableName)

    const columnList = columns.map(c => `${c.name} = EXCLUDED.${c.name}`).join(',')
    // Get the primary keys from the database
    const primaryKeys = await getTablePrimaryKeys(tableName)

    // Build query
    const query = `
        INSERT INTO ${tableName}
        SELECT * FROM ${tempTableName}
        ON CONFLICT (${primaryKeys.join(',')}) DO UPDATE
        SET ${columnList};
        `
    const truncateQuery = `TRUNCATE ${tableName} CASCADE`
    logError(query)
    try {
        await client.query('BEGIN')
        // Truncate table first if desired
        if (truncate) {
            await client.query(truncateQuery)
        }
        await client.query(query)
        await client.query('COMMIT')
        logInfo(`Batch for ${tableName} inserted!`)
    } catch (err) {
        await client.query('ROLLBACK')
        const msg = getErrorMessage(err)
        logError(`Error during upsert from '${tempTableName}' to '${tableName}':`, msg)
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

        const batches: { [tableName: string]: Array<Record<string, string>> } = {}
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
                        processRecord(currentSubject, currentRecord, currentTableName, batches)
                    }
                    recordCount++
                    currentSubject = subject
                    currentTableName = null
                    currentRecord = {}
                    logInfo(`Initiate record ${recordCount}: ${currentSubject}`)
                    if (RECORD_LIMIT && recordCount > RECORD_LIMIT) {
                        process.exit()
                    }
                }

                // Check for the record type
                if (predicate === TABLE_PRED && tables.includes(object)) {
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

                logInfo(`Processing completed: ${recordCount} records.`)
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

    if (SQUASH_GRAPHS) {
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
    }

    logInfo('--- Step 1: Construct view ---')
    console.time('Construct view')

    const queries = await addJobQueries(account, dataset)

    logInfo(`Starting pipelines for ${queries.map(q => q.slug)}.`)

    await account.runPipeline({
        destination,
        queries,
    })
    logInfo(`Pipelines completed.`)
    console.timeEnd('Construct view')

    // Parse and process the gzipped TriG file from the URL
    logInfo('--- Step 2: load temporary tables --')
    console.time('Load temporary tables')

    // Create temp tables based on recordTypeToTableMap
    const graph = await destination.dataset.getGraph(destination.graph)
    await processGraph(graph)
    console.timeEnd('Load temporary tables')

    logInfo('--- Step 3: upsert tables --')
    console.time('Upsert tables')
    for (const tableName of tables.values()) {
        const tempTableName = getTempTableName(tableName)
        // upsert records from temp table into table
        await upsertTable(tempTableName, tableName, SINCE === null)
        // drop temp table when done
        await dropTable(tempTableName)
    }
    console.timeEnd('Upsert tables')

    logInfo('--- Step 4: delete records --')
    console.time('Delete records')
    // for (const tableName of recordTypeToTableMap.values()) {
    //     const subjectsToDelete = ['subject1', 'subject2', 'subject3'];
    //     await deleteBatch(tableName, subjectsToDelete);
    // }
    console.timeEnd('Delete records')

    logInfo('--- Step 5: Graph cleanup --')
    console.time('Graph cleanup')
    for await (const graph of dataset.getGraphs()) {
        graph.delete()
    }
    console.timeEnd('Graph cleanup')
}

main().catch(err => {
    const msg = getErrorMessage(err)
    logError(msg)
    process.exit(1)
}).finally(() => pool.end())

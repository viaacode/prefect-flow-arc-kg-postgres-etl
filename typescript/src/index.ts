import { Quad } from 'rdf-js'
import { fromRdf } from 'rdf-literal'
import { parse as parseDuration, toSeconds } from "iso8601-duration"
import App from '@triply/triplydb'
import { Account } from '@triply/triplydb/Account.js'
import { AddQueryOptions } from '@triply/triplydb/commonAccountFunctions.js'
import Graph from '@triply/triplydb/Graph.js'
import { readdir, readFile } from 'fs/promises'
import { join, extname, parse } from 'path'
import Dataset from '@triply/triplydb/Dataset.js'
import {
    BATCH_SIZE, RECORD_LIMIT, QUERY_PATH, NAMESPACE,
    XSD_DURATION, ACCOUNT, DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, TOKEN,
    SINCE,
    GRAPH_BASE,
    SKIP_SQUASH,
    TABLE_PRED,
    SKIP_VIEW,
    SKIP_CLEANUP
} from './configuration.js'
import { logInfo, logError, logDebug, getErrorMessage, msToTime, logWarning } from './util.js'
import { DepGraph } from 'dependency-graph'
import { TableNode, TableInfo, Destination, GraphInfo } from './types.js'
import { closeConnectionPool, createTempTable, getTableColumns, getDependentTables, getTablePrimaryKeys, dropTable, upsertTable, processDeletes, batchInsertUsingCopy, batchCount, unprocessedBatches } from './database.js'
import { performance } from 'perf_hooks'

const tableIndex = new DepGraph<TableNode>()

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

async function createTableNode(tableName: string): Promise<TableNode> {
    const tableInfo = new TableInfo(tableName)
    const tableNode = {
        tableInfo,
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


async function processBatch(tableName: string, batch: Array<Record<string, string>>) {
    // Get table information from the table index, or create a temp table if not exists
    const tableNode = tableIndex.hasNode(tableName) ? tableIndex.getNodeData(tableName) : await createTableNode(tableName)
    // Copy the batch to database
    await batchInsertUsingCopy(tableNode, batch)
}

// Process each record and add it to the appropriate batch
async function processRecord(
    subject: string,
    record: Record<string, string>,
    tableName: string,
    batches: { [tableName: string]: Array<Record<string, string>> }
): Promise<Record<string, string>[] | undefined> {
    logDebug(`Process record for ${subject}: ${JSON.stringify(record)}`)
    // If parts are missing, do nothing
    if (!record || !tableName || !subject) return
    // Init batch for table if it does not exist yet
    if (!batches[tableName]) {
        batches[tableName] = []
    }
    // Add records to table batch
    batches[tableName].push(record)
    return batches[tableName]
}

// Main function to parse and process the gzipped TriG file from a URL
async function processGraph(graph: Graph) {
    // Retrieve total number of triples
    const { numberOfStatements } = await graph.getInfo()
    // Retrieve the graph as a stream of RDFjs objects
    const quadStream = await graph.toStream('rdf-js')
    // Wrap stream processing in a promise so we can use a simple async/await
    return new Promise<void>((resolve, reject) => {
        // Init counter for records
        let recordCount = 0
        let tripleCount = 0

        // Init the batch cache
        const batches: { [tableName: string]: Record<string, string>[] } = {}

        // Init variables that track the current subject, record and table
        let currentRecord: Record<string, string> = {}
        let currentSubject: string | null = null
        let currentTableName: string | null = null

        // Process the stream
        quadStream
            .on('data', async (quad: Quad) => {
                // Deconstruct the RDF terms to simple JS variables
                const subject = quad.subject.value
                const predicate = quad.predicate.value
                let object = quad.object.value
                let language
                // Convert literal to primitive
                if (quad.object.termType === "Literal") {
                    language = quad.object.language
                    // Turn literals to JS primitives, but convert duration to seconds first
                    object = quad.object.datatype.value === XSD_DURATION ? toSeconds(parseDuration(object)) : fromRdf(quad.object)
                }

                // If the subject changes, create a new record
                if (subject !== currentSubject) {
                    // Process the current record if there is one
                    if (currentSubject !== null && Object.keys(currentRecord).length > 0 && currentTableName !== null) {
                        try {
                            // Pause the stream so it does not prevent async processRecord function from executing
                            quadStream.pause()
                            // Add the current record to the table batch
                            const batch = await processRecord(currentSubject, currentRecord, currentTableName, batches)

                            // If the maximum batch size is reached for this table, process it
                            if (batch && batch.length >= BATCH_SIZE) {
                                const memory = process.memoryUsage()
                                logInfo(`Maximum batch size of ${BATCH_SIZE} (cur. batch: ${batches[currentTableName].length};${Object.keys(batches).length} batches) reached for ${currentTableName}; processing (${Math.round((tripleCount / numberOfStatements) * 100)}% of graph; memory: ${memory.heapUsed}/${memory.heapTotal}). `)
                                await processBatch(currentTableName, batch)
                                // empty table batch when it's processed
                                batches[currentTableName] = []
                            }

                            logDebug(`Record ${recordCount} (${currentSubject}) processed`)
                        } finally {
                            // Resume the stream after the async function is done, also when failed.
                            quadStream.resume()
                        }
                    }

                    // If a set record limit is reached, stop the RDF stream
                    if (RECORD_LIMIT && recordCount > RECORD_LIMIT) {
                        return quadStream.destroy()
                    }

                    // Increment the number of processed records
                    recordCount++

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

                    // Pick first value and ignore other values. 
                    // Workaround for languages: if the label is nl, override the existing value
                    if (currentRecord[columnName] === undefined || language === 'nl') {
                        currentRecord[columnName] = object
                    } else {
                        logWarning(`Possible unexpected additional value for ${columnName}: ${object}`, { language, currentTableName, currentSubject })
                    }
                }
                tripleCount++
            })
            // When the stream has ended
            .on('end', async () => {
                // Process the last record
                if (currentSubject && currentRecord && currentTableName) {
                    await processRecord(currentSubject, currentRecord, currentTableName, batches)
                }

                // Insert any remaining batches
                for (const tableName in batches) {
                    if (batches[tableName].length) {
                        await processBatch(tableName, batches[tableName])
                    }
                }

                logInfo(`Processing completed: ${recordCount} records (${batchCount}/${Math.ceil(recordCount / BATCH_SIZE) + tableIndex.size()} batches).`)
                // stream has been completely processed, resolve the promise.
                resolve()
            })
            .on('error', (err: Error) => {
                logError('Error during parsing or processing:', err)
                reject(err)
            })
    })
}

async function cleanup() {
    const { destination } = await getInfo()
    // Clear graphs
    logInfo(`- Clearing graphs in dataset ${destination.dataset.slug}`)
    await destination.dataset.clear("graphs")
    // Clear temp tables
    for (const tableName of tableIndex.overallOrder()) {
        const tableNode = tableIndex.getNodeData(tableName)
        logInfo(`- Dropping ${tableNode.tempTable}`)
        // drop temp table when done
        await dropTable(tableNode.tempTable)
    }
}

async function getInfo(): Promise<GraphInfo> {
    const triply = App.get({ token: TOKEN })

    const account = await triply.getAccount(ACCOUNT)
    // TODO: create dataset if not exists
    const dataset = await account.getDataset(DATASET)
    const destination: Destination = {
        dataset: await account.getDataset(DESTINATION_DATASET),
        graph: GRAPH_BASE + DESTINATION_GRAPH
    }
    return { account, dataset, destination }
}

// Main execution function
async function main() {
    logInfo(`Starting sync from ${DATASET} to ${DESTINATION_DATASET} (${DESTINATION_GRAPH})`)
    let { account, dataset, destination } = await getInfo()

    logInfo(`--- Syncing ${DATASET} to graph ${destination.graph} of ${DESTINATION_DATASET} ---`)
    let start: number
    if (!SKIP_VIEW) {
        if (!SKIP_SQUASH) {
            logInfo('--- Step 0: Squash graphs ---')
            start = performance.now()
            const graphName = GRAPH_BASE + DATASET

            await destination.dataset.clear("graphs")
            logInfo(`Cleared graphs of dataset ${DESTINATION_DATASET}.`)

            const params: AddQueryOptions = {
                dataset,
                queryString: 'CONSTRUCT WHERE { ?s ?p ?o }',
                serviceType: 'speedy',
                output: 'response',
            }
            const query = await addQuery(account, 'squash-graphs', params)
            logInfo(`Starting pipeline for ${query.slug} to ${graphName}.`)
            await query.runPipeline({
                onProgress: progress => logInfo(`Pipeline ${query.slug}: ${Math.round(progress.progress * 100)}% complete.`),
                destination: {
                    dataset: destination.dataset,
                    graph: graphName
                }
            })
            dataset = destination.dataset
            logInfo(`Squash graphs completed (${msToTime(performance.now() - start)}).`)
        } else {
            logInfo('--- Skipping squash graphs ---')
        }

        logInfo('--- Step 1: Construct view ---')
        start = performance.now()

        const queries = await addJobQueries(account, SKIP_SQUASH ? destination.dataset : dataset)

        logInfo(`Deleting destination graph ${destination.graph}.`)

        try {
            const g = await destination.dataset.getGraph(destination.graph)
            await g.delete()
        } catch (err) {
            logInfo(`Graph ${destination.graph} does not exist.\n`)
        }

        logInfo(`Starting pipelines for ${queries.map(q => q.slug)} to ${destination.graph}.`)
        await account.runPipeline({
            destination,
            onProgress: progress => logInfo(`Pipeline ${queries.map(q => q.slug)}: ${Math.round(progress.progress * 100)}% complete.`),
            queries: queries.map(q => ({
                query: q,
                // TODO report bug on 'less than one property`
                ...SINCE ? {
                    variables: {
                        since: SINCE
                    }
                } : {}
            })),
        })
        logInfo(`View construction completed (${msToTime(performance.now() - start)}).`)
    } else {
        logInfo('--- Skipping view construction ---')
    }

    // Parse and process the gzipped TriG file from the URL
    logInfo('--- Step 2: load temporary tables and delete records --')
    start = performance.now()

    // Get destination graph and process
    const graph = await destination.dataset.getGraph(destination.graph)

    await processGraph(graph)
    logInfo(`Loading completed (${msToTime(performance.now() - start)}).`)

    logInfo('--- Step 3: upsert tables --')
    start = performance.now()

    // Resolve dependencies to table graph
    tableIndex.entryNodes().forEach(tableName => {
        const node = tableIndex.getNodeData(tableName)
        node.dependencies.forEach(dependency => {
            tableIndex.addDependency(tableName, `${dependency.schema}.${dependency.name}`)
        })
    })


    // Upsert tables in the right order
    const tables = tableIndex.overallOrder()
    logInfo(`Upserting tables in order ${tables.join(', ')}.`)
    for (const tableName of tableIndex.overallOrder()) {
        const tableNode = tableIndex.getNodeData(tableName)
        // upsert records from temp table into table; truncate tables if full sync
        await upsertTable(tableNode, !SINCE)
        // drop temp table when done
        await dropTable(tableNode.tempTable)
    }
    logInfo(`Upserting completed (${msToTime(performance.now() - start)}).`)


    if (SINCE) {
        logInfo('--- Step 4: Perform deletes --')
        start = performance.now()
        await processDeletes()
        logInfo(`Deletes completed (${msToTime(performance.now() - start)}).`)
    } else {
        logInfo('--- Skipping deletes because full sync ---')
    }

    if (!SKIP_CLEANUP) {
        logInfo('--- Step 5: Graph cleanup --')
        start = performance.now()
        await cleanup()

        logInfo(`Cleanup completed (${msToTime(performance.now() - start)}).`)
    } else {
        logInfo('--- Skipping graph cleanup ---')
    }

    logInfo('--- Sync done. --')
}

main().catch(async err => {
    const msg = getErrorMessage(err)
    logError(msg)
    if (!SKIP_CLEANUP) {
        logInfo('--- Graph and table cleanup because of error --')
        await cleanup()
    } else {
        logInfo('--- Skipping graph cleanup ---')
    }
    process.exit(1)
}).finally(async () => {
    logDebug(`Unprocessed batches: ${unprocessedBatches}`)
    await closeConnectionPool()
})

// Disaster handling
process.on('SIGTERM', signal => {
    logWarning(`Process ${process.pid} received a SIGTERM signal`, signal)
    process.exit(1)
})

process.on('SIGINT', signal => {
    logWarning(`Process ${process.pid} has been interrupted`, signal)
    process.exit(1)
})

process.on('uncaughtException', err => {
    logError(`Uncaught Exception: ${err.message}`, err)
    process.exit(1)
})

process.on('unhandledRejection', (reason, promise) => {
    logError(`Unhandled rejection at ${promise}`, reason)
    process.exit(1)
})
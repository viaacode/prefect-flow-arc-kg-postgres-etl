import App from '@triply/triplydb'
import { Account } from '@triply/triplydb/Account.js'
import { AddQueryOptions } from '@triply/triplydb/commonAccountFunctions.js'
import Graph from '@triply/triplydb/Graph.js'
import { readdir, readFile } from 'fs/promises'
import { join, extname, parse } from 'path'
import Dataset from '@triply/triplydb/Dataset.js'
import {
    RECORD_LIMIT, QUERY_PATH,
    ACCOUNT, DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, TOKEN,
    SINCE,
    GRAPH_BASE,
    SKIP_SQUASH,
    SKIP_VIEW,
    SKIP_CLEANUP,
    DEBUG_MODE,
} from './configuration.js'
import { logInfo, logError, logDebug, msToTime, logWarning, stats } from './util.js'
import './debug.js'
import { DepGraph } from 'dependency-graph'
import { TableNode, TableInfo, Destination, GraphInfo, Batch } from './types.js'
import { closeConnectionPool, createTempTable, getTableColumns, getDependentTables, getTablePrimaryKeys, dropTable, upsertTable, processDeletes, batchInsert } from './database.js'
import { performance } from 'perf_hooks'
import { RecordBatcher, RecordContructor } from './stream.js'
import { createGunzip } from 'zlib'
import { createReadStream } from 'fs'
import { rm } from 'fs/promises'
import { StreamParser } from 'n3'
import { Writable } from 'stream'
import { pipeline } from 'stream/promises'
import memwatch from '@airbnb/node-memwatch'

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

async function processGraph(graph: Graph, recordLimit?: number) {
    // Retrieve total number of triples
    const { numberOfStatements } = await graph.getInfo()
    // Retrieve the graph as a stream of RDFjs objects
    //const quadStream = await graph.toStream('rdf-js')

    logInfo(`Downloading graph of ${numberOfStatements} statements.`)
    const startDownload = performance.now()
    await graph.toFile("graph.ttl.gz", { compressed: true })
    logInfo(`Download complete in ${msToTime(performance.now() - startDownload)}. Start parsing as stream (Debug mode is ${DEBUG_MODE}).`)

    const fileStream = createReadStream("graph.ttl.gz")

    // Process the stream
    const startGraph = performance.now()

    // Init components
    const recordConstructor = new RecordContructor({ limit: recordLimit }).on('warning', ({ message, language, subject }) => logWarning(message, language, subject))
    const batcher = new RecordBatcher()

    function BatchConsumer(): Writable {
        const writer = new Writable({ objectMode: true })
        stats.numberOfStatements = numberOfStatements
        writer._write = async (batch: Batch, _encoding, done) => {
            try {
                // Pause the stream so it does not prevent async processRecord function from executing
                fileStream.pause()

                // Get table information from the table index, or create a temp table if not exists
                const tableNode = tableIndex.hasNode(batch.tableName) ? tableIndex.getNodeData(batch.tableName) : await createTableNode(batch.tableName)
                // Copy the batch to database
                const start = performance.now()
                stats.unprocessedBatches++
                logDebug(`Start insert of Batch #${batch.id} (${batch.length} records)`, { paused: fileStream.isPaused() })
                await batchInsert(tableNode, batch)
                logDebug(`Batch #${batch.id} inserted; ${recordConstructor.statementIndex} of ${numberOfStatements} statements) for ${tableNode.tableInfo} inserted using ${tableNode.tempTable} (${msToTime(performance.now() - start)})!`, { paused: fileStream.isPaused() })

                // Update stats
                stats.processedBatches++
                stats.unprocessedBatches--
                stats.statementIndex = recordConstructor.statementIndex
                stats.processedRecordIndex = recordConstructor.recordIndex

                if (stats.processedBatches % 100 === 0) {
                    const progress = stats.progress
                    const timeLeft = msToTime(Math.round(((100 - progress) * (performance.now() - startGraph)) / progress))
                    logInfo(`Processed ${stats.processedRecordIndex} records (${Math.round(progress)}% of graph; est. time remaining: ${timeLeft}).`)
                    if (DEBUG_MODE) {
                        logInfo('Debug mode is on.')
                        logHeap()
                    }
                
                }
                done()
            } catch (err: any) {
                logError('Error while processing batch', err)
                stats.failedBatches++
                done(err)
            }
            finally {
                // Resume the stream after the async function is done, also when failed.
                fileStream.resume()
            }

        }
        return writer
    }

    // Init record Stream
    try {
        await pipeline(
            fileStream,
            createGunzip(), // Unzip
            new StreamParser(), // Turn into quads
            recordConstructor, // Turn into records
            batcher, // Turn into batches
            BatchConsumer())

        logInfo(`Load pipeline completed ended: ${recordConstructor.recordIndex} records processed.`)
    } catch (err) {
        logError('Error during parsing or processing', err)
        throw err
    } finally {
        // stream has been completely processed or error, remove the local copy.
        await rm("graph.ttl.gz", { force: true })

    }
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
    logInfo('--- Step 2: load temporary tables --')
    start = performance.now()

    // Get destination graph and process
    const graph = await destination.dataset.getGraph(destination.graph)

    await processGraph(graph, RECORD_LIMIT)
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

let heapDiff: memwatch.HeapDiff = new memwatch.HeapDiff();
function logHeap() {
    logDebug('Heap difference',heapDiff.end());
    heapDiff = new memwatch.HeapDiff();
}

main().catch(async err => {
    logError('Error in main function', err)
    if (!SKIP_CLEANUP) {
        logInfo('--- Graph and table cleanup because of error --')
        await cleanup()
    } else {
        logInfo('--- Skipping graph cleanup ---')
    }
    process.exit(1)
}).finally(async () => {
    logDebug('Closing connection pool')
    await closeConnectionPool()
})

// Disaster handling
process.on('SIGTERM', signal => {
    logError(`Process ${process.pid} received a SIGTERM signal`, signal)
    process.exit(1)
})

process.on('SIGINT', signal => {
    logError(`Process ${process.pid} has been interrupted`, signal)
    process.exit(1)
})

process.on('uncaughtException', err => {
    logError('Uncaught Exception', err, err.stack)
    process.exit(1)
})

process.on('unhandledRejection', (reason, promise) => {
    logError(`Unhandled rejection at ${promise}`, reason)
    process.exit(1)
})
import Graph from '@triply/triplydb/Graph.js'
import {
    RECORD_LIMIT, 
    SINCE,
    SKIP_CLEANUP,
    DEBUG_MODE,
    USE_MERGE,
} from './configuration.js'
import { logInfo, logError, logDebug, msToTime, logWarning, stats } from './util.js'
import { DepGraph } from 'dependency-graph'
import { TableNode, TableInfo, Batch, InsertRecord } from './types.js'
import { createTempTable, getTableColumns, getDependentTables, getTablePrimaryKeys, batchInsert, mergeTable, dropTable, closeConnectionPool, checkClearValueTable } from './database.js'
import { performance } from 'perf_hooks'
import { RecordBatcher, RecordContructor } from './stream.js'
import { createGunzip } from 'zlib'
import { createReadStream } from 'fs'
import { rm } from 'fs/promises'
import { StreamParser } from 'n3'
import { Writable } from 'stream'
import { pipeline } from 'stream/promises'
import memwatch from '@airbnb/node-memwatch'
import { getInfo } from './helpers.js'

// Monitor heap usage for memory leak detection
let heapDiff: memwatch.HeapDiff = new memwatch.HeapDiff()

function logHeap() {
    logDebug('Heap difference', heapDiff.end())
    heapDiff = new memwatch.HeapDiff()
}

// Create a dependency graph to store information about tables and their FK dependencies
const tableIndex = new DepGraph<TableNode>()

// Function to bundle all information about a table in one dependency graph node.
async function createTableNode(tableInfo: TableInfo): Promise<TableNode> {
    const tableNode = {
        tableInfo,
        tempTable: await createTempTable(tableInfo),
        // Get the actual columns from the database
        columns: await getTableColumns(tableInfo),
        // Get dependent tables from the database
        dependencies: await getDependentTables(tableInfo),
        // Get primary keys
        primaryKeys: await getTablePrimaryKeys(tableInfo),
        // Check if values should be cleared before inserting
        clearValue: checkClearValueTable(tableInfo),
    }
    // add node to index
    tableIndex.addNode(tableInfo.toString(), tableNode)
    return tableNode
}

// Function that processes the RDF graph that results from running the different query jobs
async function processGraph(graph: Graph, recordLimit?: number) {
    // Retrieve total number of triples
    const { numberOfStatements } = await graph.getInfo()

    // Download the graph for local processing
    logInfo(`Downloading graph of ${numberOfStatements} statements.`)
    const startDownload = performance.now()
    await graph.toFile("graph.ttl.gz", { compressed: true })
    logInfo(`Download complete in ${msToTime(performance.now() - startDownload)}. Start parsing as stream (Debug mode is ${DEBUG_MODE}).`)

    const startGraph = performance.now()

    // Create a stream to read from the file
    const fileStream = createReadStream("graph.ttl.gz")
    // Create a transform stream that turns triples into records
    const recordConstructor = new RecordContructor({ limit: recordLimit }).on('warning', ({ message, language, subject }) => logWarning(message, language, subject))
    // Create a transfrom stream that turns records into batches
    const batcher = new RecordBatcher()

    // Create a stream consumer of batches that writes each batch to the target table
    function BatchConsumer(): Writable {
        const writer = new Writable({ objectMode: true })
        stats.numberOfStatements = numberOfStatements
        writer._write = async (batch: Batch, _encoding, done) => {
            try {
                // Pause the stream so it does not prevent async processRecord function from executing
                fileStream.pause()

                // Get table information from the table index, or create a temp table if not exists
                const tableNode = tableIndex.hasNode(batch.tableInfo.toString()) ? tableIndex.getNodeData(batch.tableInfo.toString()) : await createTableNode(batch.tableInfo)
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
                stats.processedRecordIndex = InsertRecord.index

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

    // Assemble the components above in one pipeline; and
    // process the local graph as a stream of RDFjs objects
    try {
        await pipeline(
            fileStream, // Read from file
            createGunzip(), // Unzip
            new StreamParser(), // Turn into quads
            recordConstructor, // Turn into records
            batcher, // Turn into batches
            BatchConsumer())

        logInfo(`Load pipeline completed ended: ${InsertRecord.index} records processed.`)
    } catch (err) {
        logError('Error during parsing or processing', err)
        throw err
    } finally {
        // stream has been completely processed or error, remove the local copy.
        await rm("graph.ttl.gz", { force: true })

    }
}

// Helper function to remove the created temporary tables
async function cleanup() {

    // Loop over all temp tables
    for (const tableName of tableIndex.overallOrder()) {
        const tableNode = tableIndex.getNodeData(tableName)

        // Drop temp table
        logInfo(`- Dropping ${tableNode.tempTable}`)
        await dropTable(tableNode.tempTable)
    }
}

// Main execution function
async function main() {
    // Get all TriplyDB information
    let { destination } = await getInfo()

    // Init timer
    let start: number = performance.now()

    // Parse and process the gzipped TriG file from the URL
    logInfo('--- Step 1: load temporary tables --')

    // Get destination graph
    const graph = await destination.dataset.getGraph(destination.graph)

    // Process and load the view graph and wait for completion
    await processGraph(graph, RECORD_LIMIT)
    logInfo(`Loading completed (${msToTime(performance.now() - start)}).`)

    // Init timer
    start = performance.now()
    logInfo('--- Step 2: upsert tables --')

    // Add the table nodes to the depency graph to determine correct order
    tableIndex.entryNodes().forEach(tableName => {
        const node = tableIndex.getNodeData(tableName)
        node.dependencies.forEach(dependency => {
            tableIndex.addDependency(tableName, dependency.toString())
        })
    })


    // Upsert tables in the correct order
    const tables = tableIndex.overallOrder()
    logInfo(`Upserting tables in order ${tables.join(', ')}.`)
    for (const tableName of tableIndex.overallOrder()) {
        const tableNode = tableIndex.getNodeData(tableName)
        // merge records from temp table into table; truncate tables if full sync
        const startMerge = performance.now()
        const rowCount = await mergeTable(tableNode, !SINCE, USE_MERGE)
        logInfo(`Merged ${rowCount} records for table ${tableNode.tableInfo} (${msToTime(performance.now() - startMerge)} - strategy: ${!SINCE ? "TRUNCATE+INSERT" : (USE_MERGE ? "MERGE INTO" : "INSERT ON CONFLICT")})!`)
    }
    logInfo(`Upserting completed (${msToTime(performance.now() - start)}).`)

    if (!SKIP_CLEANUP) {
        logInfo('--- Step 3: Table cleanup --')
        start = performance.now()
        await cleanup()

        logInfo(`Cleanup completed (${msToTime(performance.now() - start)}).`)
    } else {
        logInfo('--- Skipping table cleanup ---')
    }

    logInfo('--- Sync done. --')
}


main().catch(async err => {
    logError('Error in main function', err)
    if (!SKIP_CLEANUP && !DEBUG_MODE) {
        logInfo('--- Table cleanup because of error --')
        await cleanup()
    } else {
        logInfo('--- Skipping table cleanup ---')
    }
    throw err; // Re-throw to ensure the error is caught by the disaster handling
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


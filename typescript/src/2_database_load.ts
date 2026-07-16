import {
    RECORD_LIMIT, 
    SINCE,
    SKIP_CLEANUP,
    DEBUG_MODE,
    USE_MERGE,
    TABLES,
} from './configuration.js'
import { logInfo, logError, logDebug, msToTime, logWarning, stats } from './util.js'
import { DepGraph } from 'dependency-graph'
import { TableNode, TableInfo, Batch, InsertRecord } from './types.js'
import { createTempTable, getTableColumns, getDependentTables, getTablePrimaryKeys, batchInsert, mergeTable, dropTable, closeConnectionPool, TEMP_deleteOrphanedTempRepresentation, TEMP_deleteOrphanedTempIncludes, getIntersectingSchemaTables } from './database.js'
import { performance } from 'perf_hooks'
import { RecordBatcher, RecordContructor } from './stream.js'
import { createReadStream } from 'fs'
import { readdir } from 'fs/promises'
import { StreamParser } from 'n3'
import { Readable, Writable } from 'stream'
import { pipeline } from 'stream/promises'
import memwatch from '@airbnb/node-memwatch'

// Monitor heap usage for memory leak detection
let heapDiff: memwatch.HeapDiff = new memwatch.HeapDiff()

function logHeap() {
    logDebug('Heap difference', heapDiff.end())
    heapDiff = new memwatch.HeapDiff()
}

async function createTtlFolderStream(folderPath: string): Promise<Readable> {
    const ttlFiles = (await readdir(folderPath, { withFileTypes: true }))
        .filter(entry => entry.isFile() && entry.name.endsWith('.ttl'))
        .map(entry => `${folderPath}/${entry.name}`)

    if (ttlFiles.length === 0) {
        throw new Error(`No .ttl files found in ${folderPath}`)
    }

    return Readable.from((async function* () {
        for (const [index, file] of ttlFiles.entries()) {
            if (index > 0) {
                yield Buffer.from('\n')
            }
            yield* createReadStream(file)
        }
    })(), { objectMode: false })
}

// Create a dependency graph to store information about tables and their FK dependencies
const tableIndex = new DepGraph<TableNode>()

// Function to bundle all information about a table in one dependency graph node.
async function createTableNode(tableInfo: TableInfo, sourceTable?: TableInfo): Promise<TableNode> {
    const resolvedSource = sourceTable ?? await createTempTable(tableInfo)

    const tableNode = {
        tableInfo,
        sourceTable: resolvedSource,
        // Get the actual columns from the database
        columns: await getTableColumns(tableInfo),
        // Get dependent tables from the database
        dependencies: await getDependentTables(tableInfo),
        // Get primary keys
        primaryKeys: await getTablePrimaryKeys(tableInfo),
        sourceTableIsTemp: sourceTable === null
    }
    // add node to index
    tableIndex.addNode(tableInfo.toString(), tableNode)
    return tableNode
}

// Function that processes the RDF graph that results from running the different query jobs
async function processGraph(recordLimit?: number) {
    const startGraph = performance.now()

    // TODO: below will produce doubles. check if that is a problem
    const fileStream = await createTtlFolderStream(".")
    // Create a transform stream that turns triples into records
    const recordConstructor = new RecordContructor({ limit: recordLimit }).on('warning', ({ message, language, subject }) => logWarning(message, language, subject))
    // Create a transfrom stream that turns records into batches
    const batcher = new RecordBatcher()

    // Create a stream consumer of batches that writes each batch to the target table
    function BatchConsumer(): Writable {
        const writer = new Writable({ objectMode: true })
        // TODO: We don't have an estimate of the total number of triples
        stats.numberOfStatements = 0
        writer._write = async (batch: Batch, _encoding, done) => {
            try {
                // Pause the stream so it does not prevent async processRecord function from executing
                fileStream.pause()

                // Get table information from the table index, or create a temp table if not exists
                const tableNode = tableIndex.hasNode(batch.tableInfo.toString()) ? tableIndex.getNodeData(batch.tableInfo.toString()) : await createTableNode(batch.tableInfo)
                if(TABLES && !TABLES.includes(tableNode.tableInfo.toString())) {
                    done()
                    return
                }
                // Copy the batch to database
                const start = performance.now()
                stats.unprocessedBatches++
                logDebug(`Start insert of Batch #${batch.id} (${batch.length} records)`, { paused: fileStream.isPaused() })
                await batchInsert(tableNode, batch)
                logDebug(`Batch #${batch.id} inserted; ${recordConstructor.statementIndex} statements) for ${tableNode.tableInfo} inserted using ${tableNode.sourceTable} (${msToTime(performance.now() - start)})!`, { paused: fileStream.isPaused() })

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
            fileStream, // Read concatenated TTL files
            new StreamParser(), // Turn into quads
            recordConstructor, // Turn into records
            batcher, // Turn into batches
            BatchConsumer())

        logInfo(`Load pipeline completed ended: ${InsertRecord.index} records processed.`)
    } catch (err) {
        logError('Error during parsing or processing', err)
        throw err
    }
}

// Helper function to remove the created temporary tables
async function cleanup() {

    // Loop over all temp tables
    for (const tableName of tableIndex.overallOrder()) {
        const tableNode = tableIndex.getNodeData(tableName)

        if (tableNode.sourceTableIsTemp) {
            // Drop temp table
            logInfo(`- Dropping ${tableNode.sourceTable}`)
            await dropTable(tableNode.sourceTable)
        }
    }
}

// Main execution function
async function main() {

    // Init timer
    let start: number = performance.now()

    // Parse and process the gzipped TriG file from the URL
    logInfo('--- Step 1: load temporary tables --')

    // Process and load the view graph and wait for completion
    await processGraph(RECORD_LIMIT)
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

    // The static tables should be merged/inserted into the graph tables.
    // But unlike the temp tables they should not be truncated/dropped afterwards
    const intersectingTables = await getIntersectingSchemaTables("static", "graph")
    for (const table of intersectingTables) {
        const staticTable = new TableInfo("static", table.name)
        await createTableNode(table, staticTable)
    }

    // Upsert tables in the correct order
    const tables = tableIndex.overallOrder()
    logInfo(`Upserting tables in order ${tables.join(', ')}.`)
    for (const tableName of tableIndex.overallOrder()) {
        const tableNode = tableIndex.getNodeData(tableName)
        // Quick fix, delete all rows from graph.temp_representation that do not have is_mediafragment_of present in graph.file (they can be null)
        if (tableNode.tableInfo.toString() === 'graph."representation"') {
            logInfo('Deleting orphaned media fragments from temp_representation')
            await TEMP_deleteOrphanedTempRepresentation()
        }
        if (tableNode.tableInfo.toString() === 'graph."includes"') {
            logInfo('Deleting orphaned includes from temp_includes')
            await TEMP_deleteOrphanedTempIncludes()
        }
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

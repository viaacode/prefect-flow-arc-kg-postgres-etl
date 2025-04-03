import App from '@triply/triplydb'
import { Account } from '@triply/triplydb/Account.js'
import { AddQueryOptions } from '@triply/triplydb/commonAccountFunctions.js'
import Graph from '@triply/triplydb/Graph.js'
import { readdir, readFile, } from 'fs/promises'
import { join, extname, parse, dirname } from 'path'
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
    USE_MERGE,
} from './configuration.js'
import { logInfo, logError, logDebug, msToTime, logWarning, stats } from './util.js'
import { DepGraph } from 'dependency-graph'
import { TableNode, TableInfo, Destination, GraphInfo, Batch, InsertRecord } from './types.js'
import { closeConnectionPool, createTempTable, getTableColumns, getDependentTables, getTablePrimaryKeys, dropTable, batchInsert, mergeTable } from './database.js'
import { performance } from 'perf_hooks'
import { RecordBatcher, RecordContructor } from './stream.js'
import { createGunzip } from 'zlib'
import { createReadStream } from 'fs'
import { rm } from 'fs/promises'
import { StreamParser } from 'n3'
import { Writable } from 'stream'
import { pipeline } from 'stream/promises'
import memwatch from '@airbnb/node-memwatch'
import { fileURLToPath } from 'url'
const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

// Create a dependency graph to store information about tables and their FK dependencies
const tableIndex = new DepGraph<TableNode>()

// Helper function to add a SPARQL Query to TriplyDB as Saved Query; needed to run them as Query Job
async function addQuery(account: Account, queryName: string, params: AddQueryOptions) {
    // Remove query if it exists
    try {
        const query = await account.getQuery(queryName)
        await query.delete()
        logInfo(`Query ${queryName} deleted.\n`)
    } catch (err) {
        logInfo(`Query ${queryName} does not exist.\n`)
    }
    return account.addQuery(queryName, params)
}

// Function to add SPARQL queries in files to TriplyDB
async function addJobQueries(account: Account, dataset: Dataset) {
    // Determine the path where queries are located
    const queryDir = join(__dirname, QUERY_PATH)
    const files = (await readdir(queryDir))
        // Only use sparql files
        .filter(f => extname(f) === '.sparql')

    const queries = []
    for (const file of files) {
        // Read the query string from file
        const filePath = join(queryDir, file)
        const queryString = await readFile(filePath, 'utf8')
        const queryName = parse(filePath).name

        // Set the query configuration
        const params: AddQueryOptions = {
            dataset,
            queryString,
            serviceType: 'speedy',
            output: 'response',
            variables: [
                // The 'since' parameter is used for incremental loading
                {
                    name: 'since',
                    required: false,
                    termType: 'Literal',
                    datatype: 'http://www.w3.org/2001/XMLSchema#dateTime',
                }
            ]
        }

        // Add the query to TriplyDB and to the list of results
        const query = await addQuery(account, queryName, params)
        queries.push(query)
    }
    // Return the created queries
    return queries
}

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
        primaryKeys: await getTablePrimaryKeys(tableInfo)
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

// Helper function to remove the created graphs and temporary tables
async function cleanup() {
    const { destination } = await getInfo()
    // Clear graphs
    logInfo(`- Clearing graphs in dataset ${destination.dataset.slug}`)
    await destination.dataset.clear("graphs")
    
    // Loop over all temp tables
    for (const tableName of tableIndex.overallOrder()) {
        const tableNode = tableIndex.getNodeData(tableName)
        
        // Drop temp table
        logInfo(`- Dropping ${tableNode.tempTable}`)
        await dropTable(tableNode.tempTable)
    }
}

// Helper function to get account, dataset and graph information from TriplyDB token
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

    // Get all TriplyDB information
    let { account, dataset, destination } = await getInfo()

    logInfo(`--- Syncing ${DATASET} to graph ${destination.graph} of ${DESTINATION_DATASET} ---`)
    let start: number
    if (!SKIP_VIEW) {
        // To improve performance, squash all graphs into one graph.
        if (!SKIP_SQUASH) {
            logInfo('--- Step 0: Squash graphs ---')
            start = performance.now()

            // Compose the name of the graph
            const graphName = GRAPH_BASE + DATASET

            // Delete all trailing graphs in the target dataset
            // TODO: this is quite destructive whem the destination is somehow the source
            // refactor to something more fine-grained
            await destination.dataset.clear("graphs")
            logInfo(`Cleared graphs of dataset ${DESTINATION_DATASET}.`)

            // Create configuration for construct query
            const params: AddQueryOptions = {
                dataset,
                queryString: 'CONSTRUCT WHERE { ?s ?p ?o }',
                serviceType: 'speedy',
                output: 'response',
            }

            // Add the query to TriplyDB
            const query = await addQuery(account, 'squash-graphs', params)

            // Run the query as pipeline/job in TriplyDB and wait for completion
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
        
        // Add all queries needed to construct the view
        const queries = await addJobQueries(account, SKIP_SQUASH ? destination.dataset : dataset)

        logInfo(`Deleting destination graph ${destination.graph}.`)

        // Delete the destination graph if it already exists
        try {
            const g = await destination.dataset.getGraph(destination.graph)
            await g.delete()
        } catch (err) {
            logInfo(`Graph ${destination.graph} does not exist.\n`)
        }

        logInfo(`Starting pipelines for ${queries.map(q => q.slug)} to ${destination.graph} ${SINCE ? `from ${SINCE}`: '(full sync)'}.`)

        // Run the queries as a pipeline in TriplyDB and wait for completion
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

    // Get destination graph
    const graph = await destination.dataset.getGraph(destination.graph)

    // Process and load the view graph and wait for completion
    await processGraph(graph, RECORD_LIMIT)
    logInfo(`Loading completed (${msToTime(performance.now() - start)}).`)

    logInfo('--- Step 3: upsert tables --')
    start = performance.now()

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
        logInfo(`Merged ${rowCount} records for table ${tableNode.tableInfo} (${msToTime(performance.now() - startMerge)} - strategy: ${!SINCE ? "TRUNCATE+INSERT" : (USE_MERGE ? "MERGE INTO": "INSERT ON CONFLICT")})!`)
    }
    logInfo(`Upserting completed (${msToTime(performance.now() - start)}).`)

    if (!SKIP_CLEANUP) {
        logInfo('--- Step 4: Graph cleanup --')
        start = performance.now()
        await cleanup()

        logInfo(`Cleanup completed (${msToTime(performance.now() - start)}).`)
    } else {
        logInfo('--- Skipping graph cleanup ---')
    }

    logInfo('--- Sync done. --')
}

// Monitor heap usage for memory leak detection
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


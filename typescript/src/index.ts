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
import { logInfo, logError, logDebug, getErrorMessage, msToTime } from './util.js'
import { DepGraph } from 'dependency-graph'
import { TableNode, TableInfo, Destination } from './types.js'
import { closeConnectionPool, createTempTable, getTableColumns, getDependentTables, getTablePrimaryKeys, dropTable, upsertTable, processDeletes, batchInsertUsingCopy, batchCount, unprocessedBatches } from './database.js'
import { performance } from 'perf_hooks';

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
    // Create temp table if not exists
    const tableNode = tableIndex.hasNode(tableName) ? tableIndex.getNodeData(tableName) : await createTableNode(tableName)
    await batchInsertUsingCopy(tableNode, batch)
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
        await processBatch(tableName, batch)
        batches[tableName] = []
    }
}

// Main function to parse and process the gzipped TriG file from a URL
async function processGraph(graph: Graph) {
    const quadStream = await graph.toStream('rdf-js')
    return new Promise<void>((resolve, reject) => {

        let recordCount = 0

        const batches: { [tableName: string]: Record<string, string>[] } = {}
        let currentRecord: Record<string, string> = {}
        let currentSubject: string | null = null
        let currentTableName: string | null = null

        quadStream
            .on('data', async (quad: Quad) => {
                const subject = quad.subject.value
                const predicate = quad.predicate.value
                let object = quad.object.value
                let language
                // Convert literal to primitive
                if (quad.object.termType === "Literal") {
                    language = quad.object.language
                    // Convert duration to seconds first
                    object = quad.object.datatype.value === XSD_DURATION ? toSeconds(parseDuration(object)) : fromRdf(quad.object)
                }

                // If the subject changes, process the current record
                if (subject !== currentSubject) {
                    if (currentSubject !== null && Object.keys(currentRecord).length > 0 && currentTableName !== null) {
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

                    // Pick first value
                    // Workaround: if label is nl, override existing value
                    if (!currentRecord[columnName] || language === 'nl') {
                        currentRecord[columnName] = object
                    }
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
                        await processBatch(tableName, batches[tableName])
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
    let start: number
    if (!SKIP_VIEW) {
        if (!SKIP_SQUASH) {
            logInfo('--- Step 0: Squash graphs ---')
            start = performance.now();
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
        start = performance.now();

        const queries = await addJobQueries(account, dataset)

        logInfo(`Starting pipelines for ${queries.map(q => q.slug)}.`)

        await account.runPipeline({
            destination,
            onProgress: progress => logInfo(`Pipeline ${queries.map(q => q.slug)}: ${Math.round(progress.progress * 100)}% complete.`),
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
        logInfo(`View construction completed (${msToTime(performance.now() - start)}).`)
    } else {
        logInfo('--- Skipping view construction ---')
    }

    // Parse and process the gzipped TriG file from the URL
    logInfo('--- Step 2: load temporary tables and delete records --')
    start = performance.now();

    // Get destination graph and process
    const graph = await destination.dataset.getGraph(destination.graph)

    await processGraph(graph)
    logInfo(`Loading completed (${msToTime(performance.now() - start)}).`)

    logInfo('--- Step 3: upsert tables --')
    start = performance.now();

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
        // upsert records from temp table into table; truncate tables if full sync
        await upsertTable(tableNode, !SINCE)
        // drop temp table when done
        await dropTable(tableNode.tempTable)
    }
    logInfo(`Upserting completed (${msToTime(performance.now() - start)}).`)


    if (SINCE) {
        logInfo('--- Step 4: Perform deletes --')
        start = performance.now();
        await processDeletes()
        logInfo(`Deletes completed (${msToTime(performance.now() - start)}).`)
    } else {
        logInfo('--- Skipping deletes because full sync ---')
    }

    if (!SKIP_CLEANUP) {
        logInfo('--- Step 5: Graph cleanup --')
        start = performance.now();
        await graph.delete()
        logInfo(`Cleanup completed (${msToTime(performance.now() - start)}).`)
    } else {
        logInfo('--- Skipping graph cleanup ---')
    }

    logInfo('--- Sync done. --')
}

main().catch(err => {
    const msg = getErrorMessage(err)
    logError(msg)
    process.exit(1)
}).finally(async () => {
    logDebug(`Unprocessed batches: ${unprocessedBatches}`)
    await closeConnectionPool()
})

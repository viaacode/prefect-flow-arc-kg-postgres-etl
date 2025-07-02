import { AddQueryOptions } from '@triply/triplydb/commonAccountFunctions.js'
import {
    DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, 
    GRAPH_BASE,
} from './configuration.js'
import { logInfo, logError, msToTime } from './util.js'
import { performance } from 'perf_hooks'
import { addQuery, getInfo } from './helpers.js'

// Main execution function
async function main() {
    logInfo(`Starting sync from ${DATASET} to ${DESTINATION_DATASET} (${DESTINATION_GRAPH})`)

    // Get all TriplyDB information
    let { account, dataset, destination } = await getInfo()

    logInfo(`--- Syncing ${DATASET} to graph ${destination.graph} of ${DESTINATION_DATASET} ---`)
    let start: number
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

    logInfo('--- Sync done. --')
}

main().catch(async err => {
    logError('Error in main function', err)
    process.exit(1)
}).finally(async () => {
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


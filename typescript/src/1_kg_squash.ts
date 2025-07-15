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
    logInfo(`Starting graph squash for sync ${DATASET} to ${DESTINATION_DATASET} (${DESTINATION_GRAPH})`)

    // Init timer
    let start: number = performance.now()

    // Get all TriplyDB information
    let { account, dataset, destination } = await getInfo()

    // Compose the name of the graph
    const graphName = GRAPH_BASE + DATASET

    // Create configuration for construct query
    const params: AddQueryOptions = {
        dataset,
        queryString: 'CONSTRUCT WHERE { ?s ?p ?o }',
        serviceType: 'speedy',
        output: 'response',
    }

    // Add the query to TriplyDB
    const query = await addQuery(account, 'squash-graphs', params)

    // Delete the destination graph if it already exists
    try {
        await destination.dataset.deleteGraph(graphName)
    } catch (err) {
        logInfo(`Graph ${destination.graph} does not exist.\n`)
    }
    logInfo(`Deleted graph ${graphName} of dataset ${DESTINATION_DATASET}.`)

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
}

main().catch(async err => {
    logError('Error in main function', err)
    process.exit(1)
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


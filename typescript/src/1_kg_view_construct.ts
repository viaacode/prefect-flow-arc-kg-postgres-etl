import {
    DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, 
    PREFIX_ID_BASE, 
    SINCE,
} from './configuration.js'
import { logInfo, logError, msToTime } from './util.js'
import { performance } from 'perf_hooks'
import { addJobQueries, getInfo } from './helpers.js'


// Main execution function
async function main() {
    logInfo(`Starting view construction for sync ${DATASET} to ${DESTINATION_DATASET} (${DESTINATION_GRAPH})`)

    // Init timer
    let start: number = performance.now()

    // Get all TriplyDB information
    let { account, destination, dataset } = await getInfo()

    // Add all queries needed to construct the view
    const queries = await addJobQueries(account,  dataset)

    logInfo(`Deleting destination graph ${destination.graph}.`)

    // Delete the destination graph if it already exists
    try {
        await destination.dataset.deleteGraph(destination.graph)
    } catch (err) {
        logInfo(`Graph ${destination.graph} does not exist.\n`)
    }

    logInfo(`Starting pipelines for ${queries.map(q => q.slug)} to ${destination.graph} ${SINCE ? `from ${SINCE}` : '(full sync)'}.`)

    // Run the queries as a pipeline in TriplyDB and wait for completion
    await account.runPipeline({
        destination,
        onProgress: progress => logInfo(`Pipeline ${queries.map(q => q.slug)}: ${Math.round(progress.progress * 100)}% complete.`),
        queries: queries.map(q => ({
            query: q,
            // TODO report bug on 'less than one property`
            ...SINCE ? {
                variables: {
                    since: SINCE,
                    prefix_id_base: PREFIX_ID_BASE
                }
            } : {}
        })),
    })
    logInfo(`View construction completed (${msToTime(performance.now() - start)}).`)
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


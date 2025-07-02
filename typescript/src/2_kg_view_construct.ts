import {
    DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, 
    SINCE,
} from './configuration.js'
import { logInfo, logError, msToTime } from './util.js'
import { performance } from 'perf_hooks'
import { addJobQueries, getInfo } from './helpers.js'


// Main execution function
async function main() {
    logInfo(`Starting sync from ${DATASET} to ${DESTINATION_DATASET} (${DESTINATION_GRAPH})`)

    // Get all TriplyDB information
    let { account, destination } = await getInfo()

    logInfo(`--- Syncing ${DATASET} to graph ${destination.graph} of ${DESTINATION_DATASET} ---`)
    let start: number
    logInfo('--- Step 1: Construct view ---')
    start = performance.now()

    // Add all queries needed to construct the view
    const queries = await addJobQueries(account,  destination.dataset)

    logInfo(`Deleting destination graph ${destination.graph}.`)

    // Delete the destination graph if it already exists
    try {
        const g = await destination.dataset.getGraph(destination.graph)
        await g.delete()
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
                    since: SINCE
                }
            } : {}
        })),
    })
    logInfo(`View construction completed (${msToTime(performance.now() - start)}).`)
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


import { logInfo, logError, msToTime } from './util.js'
import { DepGraph } from 'dependency-graph'
import { TableNode } from './types.js'
import { dropTable } from './database.js'
import { performance } from 'perf_hooks'
import { getInfo } from './helpers.js'

const tableIndex = new DepGraph<TableNode>()

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

// Main execution function
async function main() {
    let start: number
    logInfo('--- Step 4: Graph cleanup --')
    start = performance.now()
    await cleanup()

    logInfo(`Cleanup completed (${msToTime(performance.now() - start)}).`)
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


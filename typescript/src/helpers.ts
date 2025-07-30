import App from '@triply/triplydb'
import { Account } from '@triply/triplydb/Account.js'
import { AddQueryOptions } from '@triply/triplydb/commonAccountFunctions.js'
import { readdir, readFile, } from 'fs/promises'
import { join, extname, parse, dirname } from 'path'
import Dataset from '@triply/triplydb/Dataset.js'
import {
    QUERY_PATH,
    ACCOUNT, DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, TOKEN,
    GRAPH_BASE,
} from './configuration.js'
import { logInfo } from './util.js'
import { Destination, GraphInfo } from './types.js'
import { fileURLToPath } from 'url'
const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)


// Helper function to add a SPARQL Query to TriplyDB as Saved Query; needed to run them as Query Job
export async function addQuery(account: Account, queryName: string, params: AddQueryOptions) {
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
export async function addJobQueries(account: Account, dataset: Dataset) {
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
                    name: 'prefix_id_base',
                    required: false,
                    termType: 'Literal',
                    datatype: 'http://www.w3.org/2001/XMLSchema#string',
                    defaultValue: 'https://data.hetarchief.be/id/entity/'
                },
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

// Helper function to get account, dataset and graph information from TriplyDB token
export async function getInfo(): Promise<GraphInfo> {
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
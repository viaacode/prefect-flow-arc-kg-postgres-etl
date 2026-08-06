import App from '@triply/triplydb'
import { Account } from '@triply/triplydb/Account.js'
import { AddQueryOptions } from '@triply/triplydb/commonAccountFunctions.js'
import { readdir, readFile, } from 'fs/promises'
import { join, extname, parse, dirname } from 'path'
import Dataset from '@triply/triplydb/Dataset.js'
import Query from '@triply/triplydb/Query.js'
import {
    QUERY_PATH,
    ACCOUNT, DATASET, DESTINATION_DATASET, DESTINATION_GRAPH, TOKEN,
    GRAPH_BASE,
} from './configuration.js'
import { logError, logInfo } from './util.js'
import { Destination, GraphInfo, ParameterizedQuery, QueryVariables } from './types.js'
import { fileURLToPath } from 'url'
import SparqlClient from "sparql-http-client"
import internal, { PassThrough } from 'stream'
import { Quad, Stream } from 'rdf-js'
const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)


// Helper function to add a SPARQL Query to TriplyDB as Saved Query; needed to run them as Query Job
export async function addQuery(account: Account, queryName: string, params: AddQueryOptions) {

    let query: Query;
    
    // Add version if query exists
    try {
        query = await account.getQuery(queryName)

        // Explicitely set dataset to make sure there are no query job conflicts
        query.update({dataset: (await params.dataset.getInfo()).id})
    } catch (err) {
        logInfo(`Query ${queryName} does not exist; adding it.\n`)
        return account.addQuery(queryName, params)
    }


    // If the queryString did not change, don't create a new version
    if (params.queryString == await query.getString()) {
        logInfo(`Querystring of ${queryName} did not change; not adding a new version.\n`)
        return query
    }
        
    return query.addVersion({...params, ...{ldFrame: undefined}})
    
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
                },
                {
                    name: 'maintainer_id',
                    required: false,
                    termType: 'Literal',
                    datatype: 'http://www.w3.org/2001/XMLSchema#string',
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

// Function to add SPARQL queries in files
export async function addEndpointQueries() {
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
        const params = {
            queryName,
            queryString,
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
                },
                {
                    name: 'maintainer_id',
                    required: false,
                    termType: 'Literal',
                    datatype: 'http://www.w3.org/2001/XMLSchema#string',
                }
            ]
        }

        // Add the query
        queries.push(params)
    }
    // Return the created queries
    return queries
}


/**
 * Replaces parameter placeholders in a SPARQL query string with actual literal values.
 * @param query The SPARQL query containing the query string with placeholders (e.g., ?paramName).
 * @param variables An array of parameter definitions.
 * @returns The SPARQL query string with parameters replaced.
 */
export function prepareEndpointQuery(
    query: ParameterizedQuery,
    variables: QueryVariables
): string {
    let resultString = query.queryString;

    for (const param of query.variables) {
        // Construct the expected placeholder pattern, e.g., /\?paramName/g
        const placeholderRegex = new RegExp(`\\?${param.name}`, 'g');

        // Determine the replacement value and SPARQL syntax
        let replacementLiteral: string;

        // Get the value
        const val = variables[param.name] || param.defaultValue

        if (param.termType === 'Literal') {
            // Handle literal replacement, ensuring proper quoting for string types
            if (param.datatype || param.datatype === 'http://www.w3.org/2001/XMLSchema#string') {
                // For dateTime, it's common practice to use XML Schema date format in SPARQL
                replacementLiteral = `"${val}"`;
            } else {
                // General literal replacement
                replacementLiteral = `"${val}"^^<${param.datatype}>`;
            }
        } else {
            // Handle URI replacement (assuming it's a fully qualified URI)
            replacementLiteral = `<${val}>`;
        }

        // Replace all occurrences of the placeholder with the constructed literal
        resultString = resultString.replace(placeholderRegex, replacementLiteral);
    }

    return resultString;
}

export async function runEndpointPipeline(options: { endpointUrl: string, queries: { query: ParameterizedQuery, variables: QueryVariables}[]}) {
    const queries = options.queries
    const endpointUrl = options.endpointUrl
    const sparqlClient = new SparqlClient({ endpointUrl })
    const sparqlQueries = queries.map(q => prepareEndpointQuery(q.query, q.variables))
    // Run queries in sequence and concatenate the results in a single stream
    return fetchBindingsAsSingleStream(sparqlClient, sparqlQueries)
}

export async function fetchBindingsAsSingleStream(
  sparqlClient: StreamClient,
  sparqlQueries: string[] = [],
  options?: QueryOptions
): Promise<Stream<Quad> & internal.Readable> {
  const reader = new PassThrough({
    objectMode: true,
  })

  // store first triple to prevent indefinite loop
  async function fetchBindings(
    query: string): Promise<void> {

    return new Promise(async (resolve, reject) => {
      // Fetch all bindings
      try {
        const bindingsStream = await sparqlClient.query.construct(
          query,
          options
        )
        bindingsStream.on('data', (q: Quad) => {
          if (!reader.push(q)) {
            // Pausing the stream if the internal buffer is full
            bindingsStream.pause()
          }
        })
  
        bindingsStream.on('error', (error) => {
          reader.emit('error', error)
        })
  
        bindingsStream.on('end', async () => {
          const nextQuery = sparqlQueries.pop()
          if (nextQuery) {
            // Repeat the process
            try {
              await fetchBindings(nextQuery)
            }  catch (e) {
              reader.emit('error', e)
            }

          } else {
            // If fewer or more than threshold results are returned, end the stream
            reader.push(null)
          }
          resolve()
        })
      } catch(e) {
        reader.emit('error', e)
      }
    })
  }
  // Start with first query
  const nextQuery = sparqlQueries.pop()
  if (nextQuery) {
    fetchBindings(nextQuery).catch((error) => {
        logError(error, 'Error while fetching bindings')
        reader.emit("error", error)
    })
  }
  return reader
}
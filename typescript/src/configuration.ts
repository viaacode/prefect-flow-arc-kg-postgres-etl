// Directory containing the .sparql files
export const QUERY_PATH = '../queries'

// RDF constants
export const NAMESPACE = 'https://data.hetarchief.be/ns/test/'
export const GRAPH_BASE = 'https://data.hetarchief.be/graph/'
export const TABLE_PRED = `${NAMESPACE}tableName`
export const XSD_DURATION = 'http://www.w3.org/2001/XMLSchema#duration'

// ENV variables
export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE ?? '100', 10)
export const RECORD_LIMIT = process.env.RECORD_LIMIT ? parseInt(process.env.RECORD_LIMIT, 10) : null
export const SINCE = process.env.SINCE ? new Date(process.env.SINCE).toISOString() : undefined
export const SKIP_SQUASH = process.env.SKIP_SQUASH === 'True'
export const SKIP_VIEW = process.env.SKIP_VIEW === 'True'
export const ACCOUNT = process.env.TRIPLYDB_OWNER ?? 'meemoo'
export const DATASET = process.env.TRIPLYDB_DATASET ?? 'knowledge-graph'
export const DESTINATION_DATASET = process.env.TRIPLYDB_DESTINATION_DATASET || DATASET
export const DESTINATION_GRAPH = process.env.TRIPLYDB_DESTINATION_GRAPH || 'hetarchief'

export const TOKEN = process.env.TRIPLYDB_TOKEN

// PostgreSQL connection settings
export const dbConfig = {
    port: parseInt(process.env.POSTGRES_PORT ?? '5555', 10),
    database: process.env.POSTGRES_DATABASE ?? 'hetarchief',
    host: process.env.POSTGRES_HOST ?? 'localhost',
    user: process.env.POSTGRES_USERNAME ?? 'hetarchief',
    password: process.env.POSTGRES_PASSWORD ?? 'password',
    //url: process.env.POSTGRES_URL,
}





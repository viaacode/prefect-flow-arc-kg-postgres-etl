// Directory containing the .sparql files
export const QUERY_PATH = '../queries'

// RDF constants
export const NAMESPACE = 'urn:kg-to-postgres:'
export const GRAPH_BASE = 'https://data.hetarchief.be/graph/'
export const TABLE_PRED = `${NAMESPACE}tableName`
export const XSD_DURATION = 'http://www.w3.org/2001/XMLSchema#duration'

// ENV variables
export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE ?? '100', 10)
export const RECORD_LIMIT = process.env.RECORD_LIMIT ? parseInt(process.env.RECORD_LIMIT, 10) : undefined
export const SINCE = process.env.SINCE ? new Date(process.env.SINCE).toISOString() : undefined
export const SKIP_SQUASH = process.env.SKIP_SQUASH === 'True'
export const SKIP_VIEW = process.env.SKIP_VIEW === 'True'
export const SKIP_CLEANUP = process.env.SKIP_CLEANUP === 'True'
export const SKIP_LOAD = process.env.SKIP_CLEANUP === 'True'
export const LOGGING_LEVEL = process.env.LOGGING_LEVEL || undefined
export const ACCOUNT = process.env.TRIPLYDB_OWNER ?? 'meemoo'
export const DATASET = process.env.TRIPLYDB_DATASET ?? 'knowledge-graph'
export const DESTINATION_DATASET = process.env.TRIPLYDB_DESTINATION_DATASET || DATASET
export const DESTINATION_GRAPH = process.env.TRIPLYDB_DESTINATION_GRAPH || 'hetarchief'

export const TOKEN = process.env.TRIPLYDB_TOKEN

// PostgreSQL connection settings
export const dbConfig = {
    port: parseInt(process.env.POSTGRES_PORT ?? '5432', 10),
    database: process.env.POSTGRES_DATABASE ?? 'hetarchief',
    host: process.env.POSTGRES_HOST ?? 'localhost',
    user: process.env.POSTGRES_USERNAME ?? 'hetarchief',
    password: process.env.POSTGRES_PASSWORD ?? 'password',
    ssl: process.env.POSTGRES_SSL === 'True' ? {
        rejectUnauthorized: false
    } : false,
    max: parseInt(process.env.POSTGRES_POOL_MAX ?? '5', 10),
    min: parseInt(process.env.POSTGRES_POOL_MIN ?? '0', 10),
}

export const DEBUG_MODE = process.env.DEBUG_MODE === 'True'
export const USE_MERGE = (process.env.POSTGRES_USE_MERGE || 'True')  === 'True'





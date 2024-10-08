// Directory containing the .sparql files
export const QUERY_PATH = '../queries'

// RDF constants
export const NAMESPACE = 'https://data.hetarchief.be/ns/test/'
export const GRAPH_BASE = 'https://data.hetarchief.be/graph/'
export const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
export const XSD_DURATION = 'http://www.w3.org/2001/XMLSchema#duration'

// ENV variables
export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE ?? '100', 10)
export const RECORD_LIMIT = process.env.RECORD_LIMIT ? parseInt(process.env.RECORD_LIMIT, 10) : null
export const SINCE = process.env.SINCE ? new Date(process.env.SINCE) : null
export const SQUASH_GRAPHS = process.env.SQUASH_GRAPHS === 'True'
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

// Map RecordType to target tables and dynamic column configuration
export const recordTypeToTableMap: Map<string, string> = new Map(
    [
        [`${NAMESPACE}IntellectualEntityRecord`, 'graph.intellectual_entity'],
        [`${NAMESPACE}FileRecord`, 'graph.file'],
        [`${NAMESPACE}CarrierRecord`, 'graph.carrier'],
        [`${NAMESPACE}RepresentationRecord`, 'graph.representation'],
        [`${NAMESPACE}ThingRecord`, 'graph.thing'],
        [`${NAMESPACE}RoleRecord`, 'graph.schema_role'],
    ]
)




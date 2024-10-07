export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE ?? '100', 10)
export const RECORD_LIMIT = process.env.RECORD_LIMIT ? parseInt(process.env.RECORD_LIMIT, 10) : null
// Directory containing the .sparql files
export const QUERY_PATH = './queries'

export const destination = {
    account: process.env.TRIPLYDB_OWNER ?? 'meemoo',
    dataset: process.env.TRIPLYDB_DATASET ?? 'knowledge-graph',
}

// RDF namespace
export const NAMESPACE = 'https://data.hetarchief.be/ns/test/'
export const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
export const XSD_DURATION = 'http://www.w3.org/2001/XMLSchema#duration'

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

// PostgreSQL connection settings
export const dbConfig = {
    port: parseInt(process.env.POSTGRES_PORT ?? '5555', 10),
    database: process.env.POSTGRES_DATABASE ?? 'hetarchief',
    host: process.env.POSTGRES_HOST ?? 'localhost',
    user: process.env.POSTGRES_USERNAME ?? 'hetarchief',
    password: process.env.POSTGRES_PASSWORD ?? 'password',
    //url: process.env.POSTGRES_URL,
}


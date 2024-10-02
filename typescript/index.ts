import * as zlib from 'zlib'
import fr from 'follow-redirects'
const { https } = fr
import { rdfParser } from "rdf-parse"
import { Quad } from 'rdf-js'
import pg from 'pg'
import { from } from 'pg-copy-streams'
import { fromRdf } from 'rdf-literal'
import { stringify } from 'csv-stringify'
import { pipeline } from 'node:stream/promises'
import { parse, toSeconds } from "iso8601-duration"

// PostgreSQL connection settings
const dbConfig = {
    user: 'hetarchief',
    host: 'localhost',
    database: 'hetarchief',
    password: 'password',
    port: 5555,
}

// RDF namespace
const NAMESPACE = 'https://data.hetarchief.be/ns/test/'
const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
const XSD_DURATION = 'http://www.w3.org/2001/XMLSchema#duration'
const BATCH_SIZE = 100
const LIMIT = null

type TableInfo = { name: string, schema: string }

// Map RecordType to target tables and dynamic column configuration
const recordTypeToTableMap: Map<string, string> = new Map(
    [
        [`${NAMESPACE}IntellectualEntityRecord`, 'graph.intellectual_entity'],
        [`${NAMESPACE}CarrierRecord`, 'graph.carrier'],
        [`${NAMESPACE}RepresentationRecord`, 'graph.representation'],
        [`${NAMESPACE}FileRecord`, 'graph.file']
    ]
)

const columnCache: { [tableName: string]: string[] } = {}

// PostgreSQL connection pool
const pool = new pg.Pool(dbConfig)

function getTempTableName(tableName: string) {
    const tableInfo = parseTableName(tableName)
    return `${tableInfo.schema}.temp_${tableInfo.name}`
}

function parseTableName(tableName: string): TableInfo {
    const parts = tableName.split('.')
    return { name: parts[1], schema: parts[0] }
}

// Helper function to create a table dynamically based on the columns
async function createTempTable(tableName: string) {
    const tempTableName = getTempTableName(tableName)
    const query = `
        DROP TABLE IF EXISTS ${tempTableName};
        CREATE TABLE IF NOT EXISTS ${tempTableName} (LIKE ${tableName} INCLUDING ALL EXCLUDING INDEXES EXCLUDING CONSTRAINTS);
    `
    const client = await pool.connect()
    try {
        await client.query(query)
        return tempTableName
    } catch (err) {
        console.error(`Error creating temp table ${tempTableName}:`, err)
        throw err
    }
    finally {
        client.release()
    }
}

async function getTableColumnsWithCache(tableName: string): Promise<string[]> {
    if (columnCache[tableName]) {
        //console.log(`Returning ${columnCache[tableName].length} columns for ${tableName} from cache.`)
        return columnCache[tableName]
    }
    const columns = await getTableColumns(tableName)
    //console.log(`Returning ${columns} columns for ${tableName}.`)
    columnCache[tableName] = columns
    return columns
}

// Helper function to retrieve column names for a specific table
async function getTableColumns(tableName: string): Promise<string[]> {
    const client = await pool.connect()
    const query = `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1 AND table_schema = $2
    `
    console.log(query)
    try {
        const { name, schema } = parseTableName(tableName)
        const result = await client.query(query, [name, schema])
        return result.rows.map((row: { column_name: string }) => row.column_name)
    } catch (err) {
        console.error(`Error retrieving columns for table ${tableName}:`, err)
        throw err
    } finally {
        client.release()
    }
}

async function batchInsertUsingCopy(tableName: string, batch: Array<Record<string, string>>) {
    if (!batch.length) return

    const { schema, name } = parseTableName(tableName)

    console.log(`Start batch insert using COPY for ${tableName}`)

    // Get the actual columns from the database
    const columns = await getTableColumnsWithCache(tableName)

    const client = await pool.connect()
    const columnList = columns.join(',')
    const copyQuery = `COPY ${schema}."${name}" (${columnList}) FROM STDIN WITH (FORMAT csv)`

    console.log(copyQuery)

    try {
        await client.query('BEGIN')
        const ingestStream = client.query(from(copyQuery))

        // Initialize the stringifier
        const sourceStream = stringify({
            delimiter: ",",
            cast: {
                date: function (value) {
                    return value.toISOString()
                },
            },
        })

        // Convert batch to CSV format
        for (const record of batch) {
            const values = columns.map(col => record[col] || null)
            sourceStream.write(values)
        }
        sourceStream.end()
        await pipeline(sourceStream, ingestStream)
        await client.query('COMMIT')
        console.log(`Batch for ${tableName} inserted!`)
    } catch (err) {
        await client.query('ROLLBACK')
        console.error(`Error during bulk insert for table ${tableName}:`, err)
        stringify(
            batch.map(record => columns.map(col => record[col] || null)),
            console.error
        )

    } finally {
        client.release()
    }
}


// Process each record and add it to the appropriate batch
async function processRecord(
    currentSubject: string,
    currentRecord: Record<string, string>,
    currentRecordType: string,
    batches: { [tableName: string]: Array<Record<string, string>> }
) {
    //console.log(`Process record for ${currentSubject}: ${JSON.stringify(currentRecord)}`)
    const table = recordTypeToTableMap.get(currentRecordType)

    if (!currentRecord || !currentRecordType || !currentSubject || !table) return

    const tempTableName = getTempTableName(table)

    if (!batches[tempTableName]) {
        batches[tempTableName] = []
    }

    batches[tempTableName].push(currentRecord)

    if (batches[tempTableName].length >= BATCH_SIZE) {
        console.log(`Maximum batch size reached for ${tempTableName}; processing.`)
        const batch = batches[tempTableName]
        batchInsertUsingCopy(tempTableName, batch)
        batches[tempTableName] = []
    }
}

async function upsertRecords() {

}


// Main function to parse and process the gzipped TriG file from a URL
async function parseTrigGzAndInsertInBatches(url: string, token: string) {
    return new Promise<void>((resolve, reject) => {
        https.get(url, {
            headers: {
                Authorization: `Bearer ${token}`
            }
        }, (response) => {
            if (response.statusCode !== 200) {
                return reject(new Error(`Failed to fetch file: Status code ${response.statusCode}`))
            }

            let recordCount = 0

            const trigStream = response.pipe(zlib.createGunzip())

            const batches: { [tableName: string]: Array<Record<string, string>> } = {}
            let currentRecord: Record<string, string> | null = null
            let currentSubject: string | null = null
            let currentRecordType: string | null = null

            rdfParser
                .parse(trigStream, { contentType: 'application/trig' })
                .on('data', async (quad: Quad) => {
                    const subject = quad.subject.value
                    const predicate = quad.predicate.value
                    let object = quad.object.value
                    // Convert literal to primitive
                    if (quad.object.termType === "Literal") {
                        // Convert duration to seconds first
                        object = quad.object.datatype.value === XSD_DURATION ? toSeconds(parse(object)) : fromRdf(quad.object)
                    }

                    // If the subject changes, process the current record
                    if (subject !== currentSubject) {
                        if (currentSubject !== null && currentRecord && currentRecordType !== null) {
                            processRecord(currentSubject, currentRecord, currentRecordType, batches)
                        }
                        recordCount++
                        currentSubject = subject
                        currentRecordType = null
                        currentRecord = {}
                        console.log(`Initiate record ${recordCount}: ${currentSubject}`)
                        if (LIMIT && recordCount > LIMIT) {
                            process.exit()
                        }
                    }

                    // Check for the record type
                    if (predicate === RDF_TYPE && recordTypeToTableMap.has(object)) {
                        currentRecordType = object
                    }
                    // Handle predicates within the known namespace
                    else if (predicate.startsWith(NAMESPACE)) {
                        const columnName = predicate.replace(NAMESPACE, '')
                        // TODO: handle multiple values
                        currentRecord![columnName] = object
                    }
                })
                .on('end', async () => {
                    // Process the last record
                    if (currentSubject && currentRecord && currentRecordType) {
                        await processRecord(currentSubject, currentRecord, currentRecordType, batches)
                    }

                    // Insert any remaining batches
                    for (const tableName in batches) {
                        if (batches[tableName].length) {
                            await batchInsertUsingCopy(tableName, batches[tableName])
                        }
                    }

                    console.log(`Processing completed: ${recordCount} records.`)
                    resolve()
                })
                .on('error', (err: Error) => {
                    console.error('Error during parsing or processing:', err)
                    reject(err)
                })
        }).on('error', (err) => {
            console.error('Error during file download:', err)
            reject(err)
        })
    })
}

// Main execution function
async function main() {
    try {
        // Create temp tables based on recordTypeToTableMap
        for (const tableInfo of recordTypeToTableMap.values()) {
            const name = await createTempTable(tableInfo)
            await getTableColumnsWithCache(name)
        }

        // URL to the gzipped TriG file (replace with the actual URL)
        const gzippedTrigUrl = 'https://nightly.triplydb.com/deemoo/query-job-results/download.trig.gz?graph=https%3A%2F%2Fnightly.triplydb.com%2Fdeemoo%2Fquery-job-results%2Fgraphs%2Fdefault'
        const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJ1bmtub3duIiwiaXNzIjoiaHR0cHM6Ly9hcGkubmlnaHRseS50cmlwbHlkYi5jb20iLCJqdGkiOiIxNjE1OTdjNy1mZDQ1LTQ4YWYtYjI1ZC05MGJiY2NlNjI1YjIiLCJ1aWQiOiI2NjJhNTdlODkwM2JmOWJkYTczOTA1YjYiLCJpYXQiOjE3Mjc3MDYyNzh9.Su91yv-KZC0JFURdE3UVXlzRFJSQn3fe9PqIytK25Us'

        // Parse and process the gzipped TriG file from the URL
        await parseTrigGzAndInsertInBatches(gzippedTrigUrl, token)
    } catch (err) {
        console.error('Error in main function:', err)
    } finally {
        pool.end()
    }
}

main().catch(console.error)

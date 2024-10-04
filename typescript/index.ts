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
import { parse as parseDuration, toSeconds } from "iso8601-duration"
import App from '@triply/triplydb'
import { Account } from '@triply/triplydb/Account.js'
import { AddQueryOptions } from '@triply/triplydb/commonAccountFunctions.js'
import Graph from '@triply/triplydb/Graph.js'
import { PipelineProgress } from '@triply/triplydb/Pipeline.js'
import { readdir, readFile } from 'fs/promises'
import { join, extname, parse } from 'path'
import Dataset from '@triply/triplydb/Dataset.js'

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
type ColumnInfo = { name: string, datatype: string }

// Map RecordType to target tables and dynamic column configuration
const recordTypeToTableMap: Map<string, string> = new Map(
    [
        [`${NAMESPACE}IntellectualEntityRecord`, 'graph.intellectual_entity'],
        [`${NAMESPACE}FileRecord`, 'graph.file'],
        [`${NAMESPACE}CarrierRecord`, 'graph.carrier'],
        [`${NAMESPACE}RepresentationRecord`, 'graph.representation'],
        [`${NAMESPACE}ThingRecord`, 'graph.thing'],
        [`${NAMESPACE}RoleRecord`, 'graph.role'],
    ]
)

const columnCache: { [tableName: string]: ColumnInfo[] } = {}

// PostgreSQL connection pool
const pool = new pg.Pool(dbConfig)

async function constructView(account: Account, source: Dataset, destination: { dataset: Dataset, graph?: Graph }, filePath: string) {
    return new Promise(async (resolve, reject) => {
        try {
            const queryString = await readFile(filePath, 'utf8')
            const queryName = parse(filePath).name

            let query
            const params: AddQueryOptions = {
                dataset: source,
                queryString,
                serviceType: 'virtuoso',
                output: 'response',
                variables: [
                    {
                        name: 'since',
                        termType: 'Literal',
                        datatype: 'http://www.w3.org/2001/XMLSchema#dateTime'
                    }
                ]
            }

            try {
                query = await account.getQuery(queryName)
                await query.delete()
                console.log(`Query ${queryName} deleted.\n`)
            } catch (error) {
                console.log(`Query ${queryName} does not exist.\n`)
            }
            query = await account.addQuery(queryName, params)


            // job thing
            const pipeline = await query.runPipeline({
                destination,
                onProgress: (progress: PipelineProgress) => {
                    if (progress.finished) {
                        resolve(true)
                    }
                }
            })

        } catch (readErr) {
            console.error(readErr)
            reject(readErr)
        }
    })
}

function isValidDate(date: any) {
    return date && Object.prototype.toString.call(date) === "[object Date]" && !isNaN(date)
}

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
        CREATE TABLE IF NOT EXISTS ${tempTableName} (LIKE ${tableName} INCLUDING ALL EXCLUDING CONSTRAINTS);
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

async function getTableColumnsWithCache(tableName: string): Promise<ColumnInfo[]> {
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
async function getTableColumns(tableName: string): Promise<ColumnInfo[]> {
    const client = await pool.connect()
    const query = `
        SELECT column_name AS name, data_type AS datatype
        FROM information_schema.columns
        WHERE table_name = $1 AND table_schema = $2
    `
    console.log(query)
    try {
        const { name, schema } = parseTableName(tableName)
        const result = await client.query(query, [name, schema])
        return result.rows//.map((row: { column_name: string, data_type: string }) => {row.column_name})
    } catch (err) {
        console.error(`Error retrieving columns for table ${tableName}:`, err)
        throw err
    } finally {
        client.release()
    }
}

// Helper function to retrieve primary keys for a specific table
async function getTablePrimaryKeys(tableName: string): Promise<string[]> {
    const client = await pool.connect()
    const query = `
        SELECT COLUMN_NAME from information_schema.key_column_usage 
        WHERE table_name = $1 AND table_schema = $2 AND constraint_name LIKE '%pkey'
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


// Helper function to delete a batch of records based on the 'subject' column
async function deleteBatch(tableName: string, ids: string[]) {
    if (!ids.length) return

    const client = await pool.connect()
    const query = `
        DELETE FROM "${tableName}"
        WHERE id = ANY($1::text[]);
    `

    try {
        await client.query('BEGIN')
        await client.query(query, [ids])
        await client.query('COMMIT')
        console.log(`Deleted ${ids.length} records from table ${tableName}`)
    } catch (err) {
        await client.query('ROLLBACK')
        console.error(`Error during batch delete for table ${tableName}:`, err)
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
    const columnList = columns.map(c => c.name).join(',')
    const copyQuery = `COPY ${schema}."${name}" (${columnList}) FROM STDIN WITH (FORMAT csv)`

    console.log(copyQuery)

    try {
        await client.query('BEGIN')
        const ingestStream = client.query(from(copyQuery))

        // Initialize the stringifier
        const sourceStream = stringify({
            delimiter: ",",
            cast: {
                date: (value) => {
                    return value.toISOString()
                },
            },
        })

        // Convert batch to CSV format
        for (const record of batch) {
            const values = columns.map(col => {
                // Make sure value exists and that dates are valid dates
                if (!record[col.name] || (col.datatype === 'date' && !isValidDate(record[col.name])))
                    return null

                return record[col.name]
            })
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
            batch.map(record => columns.map(col => record[col.name] || null)),
            {
                cast: {
                    date: (value) => {
                        return value.toISOString()
                    },
                }
            }, (result) => {
                console.error(result)
                process.exit()
            }
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

async function upsertTable(tableName: string) {
    const client = await pool.connect()
    // Get the temp name
    const tempTableName = getTempTableName(tableName)
    // Get the actual columns from the database
    const columns = await getTableColumnsWithCache(tableName)
    
    const columnList = columns.map(c => `${c.name} = EXCLUDED.${c.name}`).join(',')
    // Get the primary keys from the database
    const primaryKeys = await getTablePrimaryKeys(tableName)

    // Build query
    const query = `
        INSERT INTO ${tableName}
        SELECT * FROM ${tempTableName}
        ON CONFLICT (${primaryKeys.join(',')}) DO UPDATE
        SET ${columnList};
        `
    console.error(query)
    try {
        await client.query('BEGIN')
        await client.query(query)
        await client.query('COMMIT')
        console.log(`Batch for ${tableName} inserted!`)
    } catch (err) {
        await client.query('ROLLBACK')
        console.error(`Error during upsert from '${tempTableName}' to '${tableName}':`, err)
    } finally {
        client.release()
    }

}


// Main function to parse and process the gzipped TriG file from a URL
async function processGraph(graph: Graph) {
    const quadStream = await graph.toStream('rdf-js')
    return new Promise<void>((resolve, reject) => {

            let recordCount = 0

            const batches: { [tableName: string]: Array<Record<string, string>> } = {}
            let currentRecord: Record<string, string> | null = null
            let currentSubject: string | null = null
            let currentRecordType: string | null = null

            quadStream
                .on('data', async (quad: Quad) => {
                    const subject = quad.subject.value
                    const predicate = quad.predicate.value
                    let object = quad.object.value
                    // Convert literal to primitive
                    if (quad.object.termType === "Literal") {
                        // Convert duration to seconds first
                        object = quad.object.datatype.value === XSD_DURATION ? toSeconds(parseDuration(object)) : fromRdf(quad.object)
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
    })
}

// Main execution function
async function main() {
    // Directory containing the .sparql files
    const directoryPath = './queries'

    const triply = App.get({ token: process.env.TOKEN })

    const account = await triply.getAccount(process.env.ACCOUNT)
    const dataset = await account.getDataset(process.env.DATASET || 'hetarchief')
    const targetGraph = await dataset.getGraph(process.env.GRAPH || 'hetarchief')

    try {
        const files = (await readdir(directoryPath))
            // Only use sparql files
            .filter(f => extname(f) === '.sparql')

        const jobResults = await Promise.all(
            files.map(
                file => {
                    const filePath = join(directoryPath, file)
                    return constructView(account, dataset, {
                        dataset, graph: targetGraph
                    }, filePath)

                }
            ))

        console.log(`Query jobs for ${files.join(',')} completed.`)

        // Create temp tables based on recordTypeToTableMap
        for (const tableInfo of recordTypeToTableMap.values()) {
            const name = await createTempTable(tableInfo)
            await getTableColumnsWithCache(name)
        }

        // Parse and process the gzipped TriG file from the URL
        console.log('--- Step 1: load temporary tables --')
        console.time('Load')
        await processGraph(targetGraph)

        console.log('--- Step 2: upsert tables --')
        console.time('Upsert')
        for (const tableName of recordTypeToTableMap.values()) {
            await upsertTable(tableName)
        }
        console.log('--- Step 3: delete records --')
        console.time('Delete')
        // for (const tableName of recordTypeToTableMap.values()) {
        //     const subjectsToDelete = ['subject1', 'subject2', 'subject3'];
        //     await deleteBatch(tableName, subjectsToDelete);
        // }

        console.timeEnd('Load')
        console.timeEnd('Upsert')
        console.timeEnd('Delete')



    } catch (err) {
        console.error('Error in main function:', err)
    } finally {
        pool.end()
    }
}

main().catch(console.error)
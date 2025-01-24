import { TableInfo, ColumnInfo, TableNode } from './types.js'
import { logInfo, logError, logDebug, stats, isValidDate } from './util.js'
import { dbConfig } from './configuration.js'
import pgplib from 'pg-promise'
import { Batch } from './stream.js'

// PostgreSQL connection pool
const pgp = pgplib({
    // Initialization Options
    capSQL: true, // capitalize all generated SQL
    error(err, e) {
        logError('Database error received', err, err.stack, e)
    }
})

// Creating a new database instance from the connection details:
const db = pgp(dbConfig)

// Helper function to create a table dynamically based on the columns
export async function createTempTable(tableInfo: TableInfo): Promise<TableInfo> {
    // Construct temp table name
    const { schema, name } = tableInfo
    const tempTableInfo = new TableInfo(schema, `temp_${name}`)

    try {
        await db.task(async t => {
            await t.none('DROP TABLE IF EXISTS $<schema:name>.$<name:name>;', tempTableInfo)
            await t.none('CREATE TABLE $<tempTableInfo.schema:name>.$<tempTableInfo.name:name> (LIKE $<tableInfo.schema:name>.$<tableInfo.name:name> INCLUDING ALL EXCLUDING CONSTRAINTS);', { tempTableInfo, tableInfo })
        })

        logDebug(`Created new temp table ${tempTableInfo} from ${tableInfo}.`)
        return tempTableInfo
    } catch (err) {
        logError(`Error creating table ${tempTableInfo}`, err)
        throw err
    }
}

export async function dropTable(tableInfo: TableInfo) {
    try {
        await db.none('DROP TABLE IF EXISTS $<schema:name>.$<name:name>;', tableInfo)
        logDebug(`Dropped table ${tableInfo} if exists.`)
        return tableInfo
    } catch (err) {
        logError(`Error dropping table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to retrieve column names for a specific table
export async function getTableColumns(tableInfo: TableInfo): Promise<ColumnInfo[]> {
    const query = `
        SELECT column_name AS name, data_type AS datatype
        FROM information_schema.columns
        WHERE table_name = $<name> AND table_schema = $<schema>
    `
    try {
        return await db.many(query, tableInfo)
    } catch (err) {
        logError(`Error retrieving columns for table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to retrieve primary keys for a specific table
export async function getDependentTables(tableInfo: TableInfo): Promise<TableInfo[]> {
    const query = `
        SELECT DISTINCT
            ccu.table_schema AS schema,
            ccu.table_name AS name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema=$<schema>
            AND tc.table_name=$<name>;
    `
    try {
        return await db.manyOrNone(query, tableInfo)
    } catch (err) {
        logError(`Error retrieving dependent tables for table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to retrieve primary keys for a specific table
export async function getTablePrimaryKeys(tableInfo: TableInfo): Promise<string[]> {

    const query = `
        SELECT COLUMN_NAME from information_schema.key_column_usage 
        WHERE table_name = $<name> AND table_schema = $<schema> AND constraint_name LIKE '%pkey'
    `
    try {
        const result = await db.many(query, tableInfo)
        return result.map((row: { column_name: string }) => row.column_name)
    } catch (err) {
        logError(`Error retrieving columns for table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to delete a batch of records
export async function processDeletes() {

    const query = `
        DELETE graph."intellectual_entity"
        FROM graph."intellectual_entity" x
        INNER JOIN graph."mh_fragment_identifier" y ON x.id = y.intellectual_entity_id
        WHERE y.is_deleted;
    `

    try {
        const result = await db.tx('process-deletes', async t => {
            await t.none(query)
            return await t.none('DELETE graph."mh_fragment_identifier" WHERE is_deleted;')
        })
        logInfo(`Deleted ${result} records from table graph."intellectual_entity" and graph."mh_fragment_identifier"`)
    } catch (err) {
        logError('Error during deletes for table graph."intellectual_entity" and graph."mh_fragment_identifier"', err)
        throw err
    }
}

export async function upsertTable(tableNode: TableNode, truncate: boolean = true) {
    const { columns, primaryKeys, tempTable, tableInfo } = tableNode

    // TODO: cleanup with assignColumns
    const { ColumnSet } = pgp.helpers
    const cs = new ColumnSet(columns.map(c => c.name))

    // Build query
    const insertQuery = pgp.as.format(`
        INSERT INTO $<tableInfo.schema:name>.$<tableInfo.name:name>
        SELECT * FROM $<tempTable.schema:name>.$<tempTable.name:name>
        ON CONFLICT ($<primaryKeys:name>) DO UPDATE `, { tableInfo, tempTable, primaryKeys })
        + cs.assignColumns({ from: 'EXCLUDED', skip: primaryKeys })

    logDebug(insertQuery)
    const truncateQuery = `TRUNCATE $<schema:name>.$<name:name> CASCADE`
    try {
        await db.tx('process-upserts', async t => {
            // Truncate table first if desired
            if (truncate) {
                await t.none(truncateQuery, tableInfo)
                logInfo(`Truncated table ${tableInfo} before upsert.`)
            }
            await t.none(insertQuery)
        })
        logInfo(`Records for table ${tableInfo} upserted!`)
    } catch (err) {
        logError(`Error during upsert from '${tempTable}' to '${tableInfo}'`, err)
        throw err
    }
}

export async function batchInsert(tableNode: TableNode, batch: Batch) {
    if (!batch.length) return

    const { columns, tempTable, tableInfo } = tableNode

    try {
        stats.unprocessedBatches++
        const { ColumnSet, insert } = pgp.helpers
        const cs = new ColumnSet(columns.map(c => ({
            name: c.name, init: (col: any) => {
                // Drop invalid date value
                if (col.exists && c.datatype === 'date' && !isValidDate(col.value)) {
                    return null
                }
                return col.value
            }
        })), {
            table: { schema: tempTable.schema, table: tempTable.name },
        })

        // generating a multi-row insert query:
        const query = insert(batch.records, cs)

        // executing the query:
        await db.none(query)

        stats.unprocessedBatches--
    } catch (err) {
        logError(`Error during bulk insert for table ${tableInfo}`, err)
        logError(`Erroreous batch (JSON)`, err, JSON.stringify(batch))
        stats.rolledbackBatches++
        throw err
    }
}

// export async function batchInsertUsingCopy(tableNode: TableNode, batch: Batch) {
//     if (!batch.length) return

//     const { columns, tempTable, tableInfo } = tableNode

//     const columnList = columns.map(c => c.name).join(',')
//     const copyQuery = `COPY ${tempTable} (${columnList}) FROM STDIN WITH (FORMAT csv)`

//     const client = await pool.connect()
//     client.on("error", (err: Error) => {
//         logError('Error received on db client', err, err.stack)
//     })
//     try {
//         stats.unprocessedBatches++
//         await client.query('BEGIN')
//         const ingestStream = client.query(from(copyQuery))
//         const sourceStream = batch.toCSVStream(columns)

//         await pipeline(sourceStream, ingestStream)
//         await client.query('COMMIT')
//         stats.unprocessedBatches--
//     } catch (err) {
//         await client.query('ROLLBACK')
//         logError(`Error during bulk insert for table ${tableInfo}`, err)
//         logError(`Erroreous batch (CSV)`, err, batch.toCSV(columns))
//         //logError(`Erroreous batch (JSON)`, err, JSON.stringify(batch))
//         stats.rolledbackBatches++
//         throw err
//     } finally {
//         client.removeAllListeners('error')
//         client.release()
//     }
// }

export async function closeConnectionPool() {
    return await db.$pool.end() // shuts down the connection pool associated with the Database object
}

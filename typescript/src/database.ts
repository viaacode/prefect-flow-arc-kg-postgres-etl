import { TableInfo, TableNode, Batch } from './types.js'
import { logInfo, logError, logDebug, isValidDate } from './util.js'
import { dbConfig } from './configuration.js'
import pgplib, { ColumnSet } from 'pg-promise'

// PostgreSQL connection pool
export const pgp = pgplib({
    // Initialization Options
    capSQL: true, // capitalize all generated SQL
    error(err, e) {
        logError('Database error received', err, err.stack, e)
    }
})

// Creating a new database instance from the connection details:
const db = pgp(dbConfig)

const qTemplates = {
    dropTable: 'DROP TABLE IF EXISTS $<schema:name>.$<name:name>;',
    createTempTable: 'CREATE UNLOGGED TABLE $<tempTableInfo.schema:name>.$<tempTableInfo.name:name> (LIKE $<tableInfo.schema:name>.$<tableInfo.name:name> INCLUDING ALL EXCLUDING CONSTRAINTS EXCLUDING INDEXES );',
    getTableColumns: `
        SELECT column_name AS name, data_type AS datatype
        FROM information_schema.columns
        WHERE table_name = $<name> AND table_schema = $<schema>`,
    getDependentTables: `
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
            AND tc.table_name=$<name>;`,
    getTablePrimaryKeys: `
        SELECT COLUMN_NAME from information_schema.key_column_usage 
        WHERE table_name = $<name> AND table_schema = $<schema> AND constraint_name LIKE '%pkey'`,
    deleteIntellectualEntitiesByFragment: `
        DELETE FROM graph."intellectual_entity" x
        USING graph."mh_fragment_identifier" y
        WHERE y.intellectual_entity_id = x.id AND y.is_deleted;`,
    deleteFragments: 'DELETE FROM graph."mh_fragment_identifier" WHERE is_deleted;',
    upsertTable: `
        INSERT INTO $<tableInfo.schema:name>.$<tableInfo.name:name>
        SELECT * FROM $<tempTable.schema:name>.$<tempTable.name:name>
        ON CONFLICT ($<primaryKeys:name>) DO UPDATE SET `,
    truncateTable: 'TRUNCATE $<schema:name>.$<name:name> CASCADE',
    renameTable:'ALTER TABLE $<schema:name>.$<from:name> RENAME TO $<schema:name>.$<to:name>;'
}

// Helper function to create a table dynamically based on the columns
export async function createTempTable(tableInfo: TableInfo): Promise<TableInfo> {
    // Construct temp table name
    const { schema, name } = tableInfo
    const tempTableInfo = new TableInfo(schema, `temp_${name}`)

    try {
        await db.task(async t => {
            await t.none(qTemplates.dropTable, tempTableInfo)
            await t.none(qTemplates.createTempTable, { tempTableInfo, tableInfo })
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
        await db.none(qTemplates.dropTable, tableInfo)
        logDebug(`Dropped table ${tableInfo} if exists.`)
        return tableInfo
    } catch (err) {
        logError(`Error dropping table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to retrieve column names for a specific table
export async function getTableColumns(tableInfo: TableInfo): Promise<ColumnSet> {
    try {
        const columns = await db.many(qTemplates.getTableColumns, tableInfo)

        return new pgp.helpers.ColumnSet(columns.map(c => ({
            name: c.name,
            init: (col: any) => {
                // Drop invalid date value
                if (col.exists && c.datatype === 'date' && !isValidDate(col.value)) {
                    return null
                }
                // Set null values in boolean fields to false
                if (c.datatype === 'boolean' && !col.exists) {
                    return false
                }
                return col.value
            },
        })))

    } catch (err) {
        logError(`Error retrieving columns for table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to retrieve primary keys for a specific table
export async function getDependentTables(tableInfo: TableInfo): Promise<TableInfo[]> {
    try {
        return await db.manyOrNone(qTemplates.getDependentTables, tableInfo)
    } catch (err) {
        logError(`Error retrieving dependent tables for table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to retrieve primary keys for a specific table
export async function getTablePrimaryKeys(tableInfo: TableInfo): Promise<string[]> {
    try {
        const result = await db.many(qTemplates.getTablePrimaryKeys, tableInfo)
        return result.map((row: { column_name: string }) => row.column_name)
    } catch (err) {
        logError(`Error retrieving columns for table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to delete a batch of records
export async function processDeletes() {
    try {
        const result = await db.tx('process-deletes', async t => {
            return {
                entities: await t.result(qTemplates.deleteIntellectualEntitiesByFragment, false),
                fragments: await t.result(qTemplates.deleteFragments, false)
            }
        })
        logInfo(`Deleted ${result.entities.rowCount} records from table graph."intellectual_entity" and ${result.fragments.rowCount} records from graph."mh_fragment_identifier"`)
    } catch (err) {
        logError('Error during deletes for table graph."intellectual_entity" and graph."mh_fragment_identifier"', err)
        throw err
    }
}

export async function upsertTable(tableNode: TableNode, truncate: boolean = true) {
    const { columns, primaryKeys, tempTable, tableInfo } = tableNode

    // Build query
    const insertQuery = pgp.as.format(qTemplates.upsertTable, { tableInfo, tempTable, primaryKeys })
        + columns.assignColumns({ from: 'EXCLUDED', skip: primaryKeys })

    logDebug(insertQuery)
    try {
        await db.tx('process-upserts', async t => {
            // Truncate table first if desired
            if (truncate) {
                await t.none(qTemplates.truncateTable, tableInfo)
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
        const { insert } = pgp.helpers

        // generating a multi-row insert query:
        const query = insert(batch.records, columns, { schema: tempTable.schema, table: tempTable.name })

        // executing the query:
        await db.none(query)
    } catch (err) {
        logError(`Error during bulk insert for table ${tableInfo}`, err)
        logError(`Erroreous batch (JSON)`, err, JSON.stringify(batch))
        throw err
    }
}

export async function closeConnectionPool() {
    return await db.$pool.end() // shuts down the connection pool associated with the Database object
}

import { TableInfo, TableNode, Batch } from './types.js'
import { logInfo, logError, logDebug, isValidDate } from './util.js'
import { dbConfig } from './configuration.js'
import pgplib, { ColumnSet } from 'pg-promise'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'
const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

// PostgreSQL connection pool
export const pgp = pgplib({
    // Initialization Options
    capSQL: true, // capitalize all generated SQL
    error(err, e) {
        logError('Database error received', err, err.stack, e)
    }
})

// Helper for linking to external query files:
function sql(file: string, options?: pgplib.IQueryFileOptions) {
    const fullPath = join(__dirname, '../queries/sql/', file)
    return new pgp.QueryFile(fullPath, options || { minify: true })
}

// Creating a new database instance from the connection details:
const db = pgp(dbConfig)

const qTemplates = {
    dropTable: sql('drop_table.sql'),
    createTempTable: sql('create_temp_table.sql'),
    getTableColumns: sql('get_table_columns.sql'),
    getDependentTables: sql('get_dependent_tables.sql'),
    getTablePrimaryKeys: sql('get_table_primary_keys.sql'),
    upsertTable: `
        INSERT INTO $<tableInfo.schema:name>.$<tableInfo.name:name>
        SELECT * FROM $<tempTable.schema:name>.$<tempTable.name:name>
        ON CONFLICT ($<primaryKeys:raw>) DO UPDATE SET `,
    truncateTable: sql('truncate_table.sql')
}


// Helper function to create a table dynamically based on the columns for incremental load
export async function createTempTable(tableInfo: TableInfo): Promise<TableInfo> {
    // Construct temp table name
    const { schema, name } = tableInfo
    const tempTableInfo = new TableInfo(schema, `temp_${name}`)

    try {
        await db.tx('create-temp-table', async t => {
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

// Helper function to drop a table
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
        return (await db.manyOrNone(qTemplates.getDependentTables, tableInfo)).map(row => new TableInfo(row.schema, row.name))
    } catch (err) {
        logError(`Error retrieving dependent tables for table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to retrieve primary keys for a specific table
export async function getTablePrimaryKeys(tableInfo: TableInfo): Promise<ColumnSet> {
    try {
        const result = await db.many(qTemplates.getTablePrimaryKeys, tableInfo)
        return new pgp.helpers.ColumnSet(result.map((row: { column_name: string }) => row.column_name))
    } catch (err) {
        logError(`Error retrieving columns for table ${tableInfo}`, err)
        throw err
    }
}

// Helper function to upsert a temp table into the final table
export async function upsertTable(tableNode: TableNode, truncate: boolean = true) {
    const { columns, primaryKeys, tempTable, tableInfo } = tableNode

    try {
        const rowCount = await db.tx('process-upserts', async t => {
            // Truncate table first if desired
            if (truncate) {
                await t.none(qTemplates.truncateTable, tableInfo)
                logInfo(`Truncated table ${tableInfo} before upsert.`)
            }

            // Build query
            const insertQuery = pgp.as.format(qTemplates.upsertTable, { tableInfo, tempTable, primaryKeys: primaryKeys.names })
                + columns.assignColumns({ from: 'EXCLUDED' })

            logDebug(insertQuery)
            // perform upsert and catch amount of upserted rows
            const rslt = await t.result(insertQuery, null, r => r.rowCount)
            // drop temp table when done
            await t.none(qTemplates.dropTable, tempTable)
            return rslt
        })
        logInfo(`Upserted ${rowCount} records for table ${tableInfo}!`)
    } catch (err) {
        logError(`Error during upsert from '${tempTable}' to '${tableInfo}'`, err)
        throw err
    }
}

// Helper function to upsert a temp table into the final table
export async function mergeTable(tableNode: TableNode, truncate: boolean = true) {
    const { columns, primaryKeys, tempTable, tableInfo } = tableNode

    try {
        const rowCount = await db.tx('process-merge', async t => {
            // Truncate table first if desired
            if (truncate) {
                await t.none(qTemplates.truncateTable, tableInfo)
                logInfo(`Truncated table ${tableInfo} before merge.`)
            }

            // Build query
            const mergeQuery = pgp.as.format(`
            MERGE INTO $<tableInfo.schema:name>.$<tableInfo.name:name> x
            USING $<tempTable.schema:name>.$<tempTable.name:name> y
            ON ${primaryKeys.assignColumns({from: 'y', to: 'x'})}
            WHEN MATCHED THEN
                UPDATE SET ${columns.assignColumns({from: 'y'})} 
            WHEN NOT MATCHED THEN
                INSERT (${columns.names}) VALUES (${columns.columns.map(c => `y.${c.escapedName}`).join(',')});
            `, { tableInfo, tempTable, primaryKeys}) 
            console.log(mergeQuery)

            // perform upsert and catch amount of upserted rows
            const rslt = await t.result(mergeQuery, null, r => r.rowCount)
            // drop temp table when done
            await t.none(qTemplates.dropTable, tempTable)
            return rslt
        })
        logInfo(`Merged ${rowCount} records for table ${tableInfo}!`)
    } catch (err) {
        logError(`Error during merge from '${tempTable}' to '${tableInfo}'`, err)
        throw err
    }
}

// Help function to insert a batch into a table
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

// shuts down the connection pool associated with the Database object
export async function closeConnectionPool() {
    return await db.$pool.end()
}

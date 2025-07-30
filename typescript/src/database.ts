import { TableInfo, TableNode, Batch } from './types.js'
import { logInfo, logError, logDebug, isValidDate } from './util.js'
import { clearValueTables, dbConfig, DEBUG_MODE } from './configuration.js'
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

// Cache all query templates from file
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
    truncateTable: sql('truncate_table.sql'),
    copyTableData: sql('copy_table_data.sql')
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
export async function mergeTable(tableNode: TableNode, truncate: boolean = true, useMerge: boolean = true) {
    const { columns, primaryKeys, tempTable, tableInfo, clearValue } = tableNode

    try {
        return await db.tx('process-merge', async t => {
            let rslt = 0
            // Truncate table first if desired
            if (truncate) {
                await t.none(qTemplates.truncateTable, tableInfo)
                logInfo(`Truncated table ${tableInfo} before merge.`)
                // Perform simple insert because table is truncated anyway
                rslt = await t.result(qTemplates.copyTableData, { to: tableInfo, from: tempTable }, r => r.rowCount)
            } else {
                // If clearValue is set, we need to clear the values in the tables for the given primarykeys in temptable before merging
                if (clearValue) {
                    // Build query to clear values in the main table
                    const clearQuery = pgp.as.format(`
                        UPDATE $<tableInfo.schema:name>.$<tableInfo.name:name>
                        SET ${columns.assignColumns({ from: 'NULL' })}
                        FROM $<tempTable.schema:name>.$<tempTable.name:name> y
                        WHERE ${primaryKeys.columns.map(c => `x.${c.escapedName} = y.${c.escapedName}`).join(' AND ')};
                    `, { tableInfo, tempTable, primaryKeys })
                    logDebug(clearQuery)
                    // Perform clearing of values in the main table
                    let clearResult = await t.result(clearQuery, null, r => r.rowCount)
                    logInfo(`Cleared ${clearResult} rows in table ${tableInfo} before merge.`)
                }
                // Build query
                const mergeQuery = useMerge ? pgp.as.format(`
                MERGE INTO $<tableInfo.schema:name>.$<tableInfo.name:name> x
                USING $<tempTable.schema:name>.$<tempTable.name:name> y
                ON ${primaryKeys.columns.map(c => `x.${c.escapedName} = y.${c.escapedName}`).join(' AND ')}
                WHEN MATCHED THEN
                    UPDATE SET ${columns.assignColumns({ from: 'y' })} 
                WHEN NOT MATCHED THEN
                    INSERT (${columns.names}) VALUES (${columns.columns.map(c => `y.${c.escapedName}`).join(',')});
                `, { tableInfo, tempTable, primaryKeys }) : pgp.as.format(qTemplates.upsertTable, { tableInfo, tempTable, primaryKeys: primaryKeys.names })
                + columns.assignColumns({ from: 'EXCLUDED' })

                logDebug(mergeQuery)

                // Perform merge/upsert and catch amount of merged rows
                rslt = await t.result(mergeQuery, null, r => r.rowCount)
            }

            // Drop temp table when done
            if(!DEBUG_MODE) {
                await t.none(qTemplates.dropTable, tempTable)
                logInfo(`Dropped temporary table ${tempTable} after merge.`)
            } else {
                logDebug(`DEBUG_MODE is enabled, not dropping temporary table ${tempTable}.`)
            }
            return rslt
        })
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

export function checkClearValueTable(tableInfo: TableInfo): boolean {
    // Check if the table is in the list of tables to clear values
    return clearValueTables.includes(tableInfo.name)
}

// shuts down the connection pool associated with the Database object
export async function closeConnectionPool() {
    return await db.$pool.end()
}

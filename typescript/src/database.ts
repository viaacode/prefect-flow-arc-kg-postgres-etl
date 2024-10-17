import { TableInfo, ColumnInfo, TableNode } from './types.js'
import { logInfo, getErrorMessage, logError, logDebug, isValidDate } from './util.js'
import { from } from 'pg-copy-streams'
import { stringify } from 'csv-stringify'
import { stringify as stringifySync } from 'csv-stringify/sync'
import { pipeline } from 'node:stream/promises'
import { dbConfig } from './configuration.js'
import pg from 'pg'

// PostgreSQL connection pool
const pool = new pg.Pool(dbConfig)

export let unprocessedBatches = 0
export let batchCount = 0

// Helper function to create a table dynamically based on the columns
export async function createTempTable(tableInfo: TableInfo): Promise<TableInfo> {
    const client = await pool.connect()
    // Construct temp table name
    const { schema, name } = tableInfo
    const tempTableInfo = new TableInfo(schema, `temp_${name}`)

    logInfo(`Creating temp table ${tempTableInfo} from ${tableInfo} if not exists.`)
    const query = `
        DROP TABLE IF EXISTS ${tempTableInfo};
        CREATE TABLE ${tempTableInfo} (LIKE ${tableInfo} INCLUDING ALL EXCLUDING CONSTRAINTS);
    `
    try {
        await client.query(query)
        return tempTableInfo
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error creating table ${tempTableInfo}:`, msg)
        throw err
    }
    finally {
        client.release()
    }
}

export async function dropTable(tableInfo: TableInfo) {
    const client = await pool.connect()
    logInfo(`Dropping table ${tableInfo} if exists.`)
    const query = `
        DROP TABLE IF EXISTS ${tableInfo};
    `
    try {
        await client.query(query)
        return tableInfo
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error dropping table ${tableInfo}:`, msg)
        throw err
    }
    finally {
        client.release()
    }
}

// Helper function to retrieve column names for a specific table
export async function getTableColumns(tableInfo: TableInfo): Promise<ColumnInfo[]> {
    const client = await pool.connect()
    const query = `
        SELECT column_name AS name, data_type AS datatype
        FROM information_schema.columns
        WHERE table_name = $1 AND table_schema = $2
    `
    logDebug(query)
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error retrieving columns for table ${tableInfo}:`, msg)
        throw err
    } finally {
        client.release()
    }
}

// Helper function to retrieve primary keys for a specific table
export async function getDependentTables(tableInfo: TableInfo): Promise<TableInfo[]> {
    const client = await pool.connect()
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
            AND tc.table_schema=$2
            AND tc.table_name=$1;
    `
    logDebug(query)
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error retrieving dependent tables for table ${tableInfo}:`, msg)
        throw err
    } finally {
        client.release()
    }
}

// Helper function to retrieve primary keys for a specific table
export async function getTablePrimaryKeys(tableInfo: TableInfo): Promise<string[]> {
    const client = await pool.connect()
    const query = `
        SELECT COLUMN_NAME from information_schema.key_column_usage 
        WHERE table_name = $1 AND table_schema = $2 AND constraint_name LIKE '%pkey'
    `
    logDebug(query)
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows.map((row: { column_name: string }) => row.column_name)
    } catch (err) {
        const msg = getErrorMessage(err)
        logError(`Error retrieving columns for table ${tableInfo}:`, msg)
        throw err
    } finally {
        client.release()
    }
}

// Helper function to delete a batch of records
export async function processDeletes() {
    const client = await pool.connect()
    const query = `
        DELETE graph."intellectual_entity"
        FROM graph."intellectual_entity" x
        INNER JOIN graph."mh_fragment_identifier" y ON x.id = y.intellectual_entity_id
        WHERE y.is_deleted;
        DELETE graph."mh_fragment_identifier" WHERE is_deleted;
    `
    try {
        await client.query('BEGIN')
        const result = await client.query(query)
        await client.query('COMMIT')
        logInfo(`Deleted ${result} records from table graph."intellectual_entity" and graph."mh_fragment_identifier"`)
    } catch (err) {
        await client.query('ROLLBACK')
        logError(`Error during deletes for table graph."intellectual_entity" and graph."mh_fragment_identifier":`, err)
    } finally {
        client.release()
    }
}

export async function upsertTable(tableNode: TableNode, truncate: boolean = true) {
    const client = await pool.connect()
    // Get the actual columns from the database
    const { columns, primaryKeys, tempTable, tableInfo } = tableNode
    const columnList = columns.map(c => `${c.name} = EXCLUDED.${c.name}`).join(',')

    // Build query
    const query = `
        INSERT INTO ${tableInfo}
        SELECT * FROM ${tempTable}
        ON CONFLICT (${primaryKeys.join(',')}) DO UPDATE
        SET ${columnList};
        `
    const truncateQuery = `TRUNCATE ${tableInfo} CASCADE`
    logDebug(query)
    try {
        await client.query('BEGIN')
        // Truncate table first if desired
        if (truncate) {
            await client.query(truncateQuery)
        }
        await client.query(query)
        await client.query('COMMIT')
        logInfo(`Records for table ${tableInfo} upserted!`)
    } catch (err) {
        await client.query('ROLLBACK')
        const msg = getErrorMessage(err)
        logError(`Error during upsert from '${tempTable}' to '${tableInfo}':`, msg)
        throw err
    } finally {
        client.release()
    }

}

export async function batchInsertUsingCopy(tableNode: TableNode, batch: Array<Record<string, string>>) {
    if (!batch.length) return

    unprocessedBatches++

    const { columns, tempTable, tableInfo } = tableNode

    logInfo(`Start batch insert using COPY for ${tableInfo} using ${tempTable}`)

    const columnList = columns.map(c => c.name).join(',')
    const copyQuery = `COPY ${tempTable} (${columnList}) FROM STDIN WITH (FORMAT csv)`

    logDebug(copyQuery)
    const client = await pool.connect()
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
        batchCount++
        logInfo(`Batch #${batchCount} for ${tableInfo} inserted!`)
    } catch (err) {
        await client.query('ROLLBACK')
        //TODO: fix error caused by logging
        const msg = getErrorMessage(err)
        logError(`Error during bulk insert for table ${tableInfo}:`, msg)
        const result = stringifySync(
            batch.map(record => columns.map(col => record[col.name] || null)),
            {
                cast: {
                    date: (value) => {
                        return value.toISOString()
                    },
                }
            }
        )
        logError(`Erroreous batch:`, result)
        throw err
    } finally {
        client.release()
        unprocessedBatches--
    }
}

export async function closeConnectionPool() {
    return await pool.end()
}

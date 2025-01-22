import { TableInfo, ColumnInfo, TableNode } from './types.js'
import { logInfo, logError, logDebug, stats } from './util.js'
import { from } from 'pg-copy-streams'


import { pipeline } from 'node:stream/promises'
import { dbConfig } from './configuration.js'
import pg from 'pg'
import { Batch } from './stream.js'

// PostgreSQL connection pool
const pool = new pg.Pool(dbConfig)

pool.on("error", (err: Error) => {
    logError('Error received on db pool', err, err.stack)
})


// Helper function to create a table dynamically based on the columns
export async function createTempTable(tableInfo: TableInfo): Promise<TableInfo> {
    const client = await pool.connect()
    // Construct temp table name
    const { schema, name } = tableInfo
    const tempTableInfo = new TableInfo(schema, `temp_${name}`)

    const query = `
        DROP TABLE IF EXISTS ${tempTableInfo};
        CREATE TABLE ${tempTableInfo} (LIKE ${tableInfo} INCLUDING ALL EXCLUDING CONSTRAINTS);
    `
    try {
        await client.query(query)
        logDebug(`Created new temp table ${tempTableInfo} from ${tableInfo}.`)
        return tempTableInfo
    } catch (err) {
        logError(`Error creating table ${tempTableInfo}`, err)
        throw err
    }
    finally {
        client.release()
    }
}

export async function dropTable(tableInfo: TableInfo) {
    const client = await pool.connect()
    const query = `
        DROP TABLE IF EXISTS ${tableInfo};
    `
    try {
        await client.query(query)
        logDebug(`Dropped table ${tableInfo} if exists.`)
        return tableInfo
    } catch (err) {
        logError(`Error dropping table ${tableInfo}`, err)
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
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows
    } catch (err) {
        logError(`Error retrieving columns for table ${tableInfo}`, err)
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
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows
    } catch (err) {
        logError(`Error retrieving dependent tables for table ${tableInfo}`, err)
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
    try {
        const { name, schema } = tableInfo
        const result = await client.query(query, [name, schema])
        return result.rows.map((row: { column_name: string }) => row.column_name)
    } catch (err) {
        logError(`Error retrieving columns for table ${tableInfo}`, err)
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
        logError('Error during deletes for table graph."intellectual_entity" and graph."mh_fragment_identifier"', err)
        throw err
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
            logInfo(`Truncating table ${tableInfo} before upsert.`)
            await client.query(truncateQuery)
        }
        await client.query(query)
        await client.query('COMMIT')
        logInfo(`Records for table ${tableInfo} upserted!`)
    } catch (err) {
        await client.query('ROLLBACK')
        logError(`Error during upsert from '${tempTable}' to '${tableInfo}'`, err)
        throw err
    } finally {
        client.release()
    }

}

export async function batchInsertUsingCopy(tableNode: TableNode, batch: Batch) {
    if (!batch.length) return

    const { columns, tempTable, tableInfo } = tableNode

    const columnList = columns.map(c => c.name).join(',')
    const copyQuery = `COPY ${tempTable} (${columnList}) FROM STDIN WITH (FORMAT csv)`

    const client = await pool.connect()
    client.on("error", (err: Error) => {
        logError('Error received on db client', err, err.stack)
    })
    try {
        await client.query('BEGIN')
        const ingestStream = client.query(from(copyQuery))
        const sourceStream = batch.toCSVStream(columns)
        
        await pipeline(sourceStream, ingestStream)
        await client.query('COMMIT')
    } catch (err) {
        await client.query('ROLLBACK')
        logError(`Error during bulk insert for table ${tableInfo}`, err)
        logError(`Erroreous batch (CSV)`, err, batch.toCSV(columns))
        //logError(`Erroreous batch (JSON)`, err, JSON.stringify(batch))
        stats.rolledbackBatches++
        throw err
    } finally {
        client.removeAllListeners('error')
        client.release()
    }
}

export async function closeConnectionPool() {
    return await pool.end()
}

import Dataset from '@triply/triplydb/Dataset.js'
import Org from '@triply/triplydb/Org.js'
import User from '@triply/triplydb/User.js'
import { ColumnSet } from 'pg-promise'

// Class to store basic table information
export class TableInfo {
    private _name: string
    private _schema: string

    constructor(schema: string, name?: string) {
        if (!name) {
            const parts = schema.split('.')
            name = parts[1]
            schema = parts[0]
        }

        this._schema = schema
        this._name = name
    }

    public get schema() {
        return this._schema
    }

    public get name() {
        return this._name
    }

    public toString = (): string => {
        return `${this._schema}."${this._name}"`
    }
}

// Class to store a record
export class InsertRecord {
    private static _index: number = 0
    private _id: number
    public tableName: string | null = null
    public values: Record<string, string> = {}

    constructor() {
        // use static counter to count the amount of created records
        this._id = InsertRecord._index++
    }

    public get id(): number {
        return this._id
    }

    public static get index() {
        return InsertRecord._index
    }

    public toString() {
        return JSON.stringify(this)
    }
}

// Class to store a batch of records
export class Batch {
    private _tableInfo: TableInfo
    private _records: Record<string, string>[] = []
    private static _index: number = 0
    private _id: number

    constructor(tableName: string) {
        this._tableInfo = new TableInfo(tableName)
        // Static counter for the amount to created batches
        this._id = Batch._index++
    }

    public get tableInfo(): TableInfo {
        return this._tableInfo
    }

    /**
     * Return all records in batch
     */
    public get records(): Record<string, string>[] {
        return this._records
    }

    public get id(): number {
        return this._id
    }

    public static get index() {
        return Batch._index
    }

    /**
     * Add a record to the batch
     * @param record 
     */
    public add(record: InsertRecord) {
        this._records.push(record.values)
    }
    /**
     * Return the current number of records in the batch
     */
    public get length(): number {
        return this._records.length
    }

    public toString() {
        return JSON.stringify(this)
    }

}

// Helper types
export type TableNode = { tableInfo: TableInfo, tempTable: TableInfo, columns: ColumnSet, dependencies: TableInfo[], primaryKeys: ColumnSet, clearValue: boolean }
export type Destination = { dataset: Dataset, graph: string }
export type GraphInfo = { account: User | Org, dataset: Dataset, destination: Destination, }

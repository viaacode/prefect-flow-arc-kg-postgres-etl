import Dataset from '@triply/triplydb/Dataset.js'
import Org from '@triply/triplydb/Org.js'
import User from '@triply/triplydb/User.js'
import { ColumnSet } from 'pg-promise'

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

export class InsertRecord {
    private static _index: number = 0
    private _id: number
    public tableName: string | null = null
    public values: Record<string, string> = {}

    constructor() {
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

export class Batch {
    private _tableInfo: TableInfo
    private _records: Record<string, string>[] = []
    private static _index: number = 0
    private _id: number

    constructor(tableName: string) {
        this._tableInfo = new TableInfo(tableName)
        this._id = Batch._index++
    }

    public get tableInfo(): TableInfo {
        return this._tableInfo
    }

    public get records(): Record<string, string>[] {
        return this._records
    }

    public get id(): number {
        return this._id
    }

    public static get index() {
        return Batch._index
    }

    public add(record: InsertRecord) {
        this._records.push(record.values)
    }

    public get length(): number {
        return this._records.length
    }

    public toString() {
        return JSON.stringify(this)
    }

}

export type TableNode = { tableInfo: TableInfo, tempTable: TableInfo, columns: ColumnSet, dependencies: TableInfo[], primaryKeys: string[] }
export type Destination = { dataset: Dataset, graph: string }
export type GraphInfo = { account: User | Org, dataset: Dataset, destination: Destination, }

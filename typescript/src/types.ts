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

// export class ColumnInfo {
//     private _column: Column
//     private _datatype: String

//     /**
//      *
//      */
//     constructor(name: string, datatype: string) {
//         this._column = new Column({
//             name,
//             init: (col: any) => {
//                 // Drop invalid date value
//                 if (col.exists && col.cast === 'date' && !isValidDate(col.value)) {
//                     return null
//                 }
//                 return col.value
//             },
//             cast: datatype
//         })
//         this._datatype = datatype
//     }

//     public get name(): String {
//         return this._column.name
//     }

//     public get column(): Column {
//         return this._column
//     }

//     public get datatype() {
//         return this._datatype
//     }
// }

export class InsertRecord {
    public tableName: string | null = null
    public values: Record<string, string> = {}
}

export class Batch {
    private _tableName: string
    private _records: Record<string, string>[] = []
    private static _batchIndex: number = 0
    private _id: number

    constructor(tableName: string) {
        this._tableName = tableName
        this._id = Batch._batchIndex++
    }

    public get tableName(): string {
        return this._tableName
    }

    public get records(): Record<string, string>[] {
        return this._records
    }

    public get id(): number {
        return this._id
    }

    public static get batchIndex() {
        return Batch._batchIndex
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

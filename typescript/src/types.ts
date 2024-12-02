import Dataset from '@triply/triplydb/Dataset.js'
import Org from '@triply/triplydb/Org.js'
import User from '@triply/triplydb/User.js'

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

export type ColumnInfo = { name: string, datatype: string }
export type TableNode = { tableInfo: TableInfo, tempTable: TableInfo, columns: ColumnInfo[], dependencies: TableInfo[], primaryKeys: string[] }
export type Destination = { dataset: Dataset, graph: string }
export type GraphInfo = { account : User | Org, dataset: Dataset, destination: Destination, }

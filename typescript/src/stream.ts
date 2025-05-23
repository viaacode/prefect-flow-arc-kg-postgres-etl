import { Literal, Quad } from 'rdf-js'
import { Transform, TransformCallback } from 'stream'
import { BATCH_SIZE, NAMESPACE, TABLE_PRED, XSD_DURATION } from './configuration.js'
import { parse as parseDuration, toSeconds } from "iso8601-duration"
import { fromRdf } from 'rdf-literal'
import { Batch, InsertRecord } from './types.js'

// Transform stream that turns quads/triples into records
export class RecordContructor extends Transform {

    // Init variables that track the current subject, record and table
    private currentRecord: InsertRecord = new InsertRecord()
    private currentSubject: string | null = null

    private _statementIndex = 0
    private _offset: number
    private _limit?: number

    constructor(options: { offset?: number, limit?: number }) {
        super({ objectMode: true, highWaterMark: 64 })
        this._offset = options.offset || 0
        this._limit = options.limit
    }

    public get statementIndex() {
        return this._statementIndex
    }

    private _parseValue(literal: Literal) {
        if (literal.datatype.value === XSD_DURATION)
            return toSeconds(parseDuration(literal.value))

        const value = fromRdf(literal)

        if (value instanceof Date) {
            if (value.getUTCFullYear() < 1) {
                value.setUTCFullYear(1)
            }
        }

        return value
    }

    /**
     * Pivots quads with the same subject into a record until subject changes
     * @param quad 
     * @param _encoding 
     * @param cb 
     * @returns 
     */
    _transform(quad: Quad, _encoding: string, cb: Function) {
        // Skip statements until the offset is reached
        if (this._statementIndex < this._offset) {
            this._statementIndex++
            return cb()
        }

        // If a set record limit is reached, stop the RDF stream
        if (this._limit && InsertRecord.index > this._limit) {
            this.destroy()
            return cb()
        }

        // Deconstruct the RDF terms to simple JS variables
        const subject = quad.subject.value
        const predicate = quad.predicate.value
        let object = quad.object.value
        // Do extra processing on literals
        let language
        // Convert literal to primitive
        if (quad.object.termType === "Literal") {
            language = quad.object.language
            // Turn literals to JS primitives, but convert duration to seconds first
            object = this._parseValue(quad.object)
        }

        // If the subject changes, create a new record
        if (subject !== this.currentSubject) {
            // Process the current record if there is one
            if (this.currentSubject !== null && Object.keys(this.currentRecord).length > 0) {
                this.push(this.currentRecord)
            }

            // Reset the record
            this.currentSubject = subject
            this.currentRecord = new InsertRecord()
        }

        // Check for the record type
        if (predicate === TABLE_PRED) {
            this.currentRecord.tableName = object
        }
        // Handle predicates within the known namespace
        else if (predicate.startsWith(NAMESPACE)) {
            const columnName = predicate.replace(NAMESPACE, '')

            // Pick first value and ignore other values. 
            // Workaround for languages: if the label is nl, override the existing value
            if (this.currentRecord.values[columnName] === undefined || language === 'nl') {
                this.currentRecord.values[columnName] = object
            } else {
                this.emit('warning', { message: `Possible unexpected additional value for ${columnName}: ${object}`, language, subject })
            }
        }
        this._statementIndex++
        return cb()
    }

    _flush(cb: TransformCallback) {
        // push the last record from buffer
        if (this.currentSubject && this.currentRecord) {
            this.push(this.currentRecord)
        }
        cb()
    }

}

// Transform stream turning records into batches
export class RecordBatcher extends Transform {

    // Bactch buffer per table
    private batches: { [tableName: string]: Batch } = {}

    constructor() {
        super({ objectMode: true })
    }

    _transform(record: InsertRecord, _encoding: string, cb: TransformCallback) {

        // If parts are missing, do nothing
        if (!record.values || !record.tableName) {
            return cb(new Error(`Invalid record: ${record}`))
        }
        // Init batch for table if it does not exist yet
        if (!this.batches[record.tableName]) {
            this.batches[record.tableName] = new Batch(record.tableName)
        }
        // Add records to table batch
        this.batches[record.tableName].add(record)

        // If the maximum batch size is reached for this table, process it
        const batch = this.batches[record.tableName]
        if (batch && batch.length >= BATCH_SIZE) {
            this.push(batch)
            this.batches[record.tableName] = new Batch(record.tableName)
        }
        cb()
    }

    _flush(cb: TransformCallback): void {
        // Insert any remaining batches
        for (const batch of Object.values(this.batches)) {
            if (batch.length) {
                this.push(batch)
            }
        }
        cb()
    }
}
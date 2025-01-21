import { Quad } from 'rdf-js'
import { Transform } from 'stream'
import { NAMESPACE, XSD_DURATION } from './configuration.js'
import { parse as parseDuration, toSeconds } from "iso8601-duration"
import { fromRdf } from 'rdf-literal'


export class RecordContructor extends Transform {

    // Init variables that track the current subject, record and table
    private currentRecord: Record<string, string> = {}
    private currentSubject: string | null = null

    private _quadIndex = 0
    private _offset: number

    constructor(offset: number = 0) {
        super({ objectMode: true })
        this._offset = offset
    }

    public get quadIndex() {
        return this._quadIndex
    }

    _transform(quad: Quad, _encoding: string, cb: Function) {
        if (this._quadIndex < this._offset) {
            this._quadIndex++
            return cb()
        }
            
        // Deconstruct the RDF terms to simple JS variables
        const subject = quad.subject.value
        const predicate = quad.predicate.value
        let object = quad.object.value
        let language
        // Convert literal to primitive
        if (quad.object.termType === "Literal") {
            language = quad.object.language
            // Turn literals to JS primitives, but convert duration to seconds first
            object = quad.object.datatype.value === XSD_DURATION ? toSeconds(parseDuration(object)) : fromRdf(quad.object)
        }

        // If the subject changes, create a new record
        if (subject !== this.currentSubject) {
            // Process the current record if there is one
            if (this.currentSubject !== null && Object.keys(this.currentRecord).length > 0) {
                this.push(this.currentRecord)
            }

            this.currentSubject = subject
            this.currentRecord = {}
        }

        // Handle predicates within the known namespace
        if (predicate.startsWith(NAMESPACE)) {
            const columnName = predicate.replace(NAMESPACE, '')

            // Pick first value and ignore other values. 
            // Workaround for languages: if the label is nl, override the existing value
            if (this.currentRecord[columnName] === undefined || language === 'nl') {
                this.currentRecord[columnName] = object
            } else {
                this.emit('warning', {message: `Possible unexpected additional value for ${columnName}: ${object}`, language, subject})
            }
        }
        this._quadIndex++
        return cb()
    }

    _flush(cb: Function) {
        if (this.currentSubject && this.currentRecord) {
            this.push(this.currentRecord)
        }
        this.currentRecord = {}

        cb()
    }

}
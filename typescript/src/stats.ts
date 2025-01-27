export class Stats {
    processedRecordIndex = 0 // The last record index that was successfully loaded into the database
    statementIndex = 0 // The current statement index
    processedBatches = 0
    unprocessedBatches = 0
    failedBatches = 0
    numberOfStatements = 0

    public get progress(): number {
        return this.numberOfStatements > 0 ? Math.round((this.statementIndex / this.numberOfStatements) * 100) : -1
    }

    public get avgNumberOfStatements(): number {
        return this.statementIndex / this.processedRecordIndex
    }
}
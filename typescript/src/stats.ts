export class Stats {
    recordIndex = 0 // The current record index
    processedRecordIndex = 0 // The last record index that was successfully loaded into the database
    statementIndex = 0 // The current statement index
    batchIndex = 0
    unprocessedBatches = 0
    numberOfStatements = 0
    batchRollbacks = 0

    public getProgress(): number {
        return this.numberOfStatements > 0 ? (this.statementIndex / this.numberOfStatements) * 100 : -1
    }
    
    public getAvgNumberOfStatements(): number {
        return this.statementIndex / this.recordIndex
    }
}
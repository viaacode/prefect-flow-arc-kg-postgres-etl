export class Stats {
    recordIndex = 0 // The current record index
    processedRecordIndex = 0 // The last record index that was successfully loaded into the database
    statementIndex = 0 // The current statement index
    processedBatches = 0
    unprocessedBatches = 0
    rolledbackBatches = 0
    numberOfStatements = 0

    public getProgress(): number {
        return this.numberOfStatements > 0 ? (this.statementIndex / this.numberOfStatements) * 100 : -1
    }
    
    public getAvgNumberOfStatements(): number {
        return this.statementIndex / this.recordIndex
    }

    public reset() {
        this.recordIndex = 0
        this.processedRecordIndex = 0
        this.statementIndex = 0
        this.processedBatches = 0
        this.unprocessedBatches = 0
        this.rolledbackBatches = 0
        this.numberOfStatements = 0
    }
}
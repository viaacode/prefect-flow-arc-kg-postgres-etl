export function logInfo(message: any, ...optionalParams: any[]) {
    logPrefect('INFO', message, optionalParams)
}

export function logError(message: any, ...optionalParams: any[]) {
    logPrefect('ERROR', message, optionalParams)
}

export function logDebug(message: any, ...optionalParams: any[]) {
    logPrefect('DEBUG', message, optionalParams)
}

export function logPrefect(level: "INFO" | "ERROR" | "DEBUG" | "WARNING", message: any, ...optionalParams: any[]) {
    console.log(JSON.stringify({ PREFECT: { level, message, optionalParams } }))
}

export function getErrorMessage(e: unknown): string | undefined{
    if (typeof e === "string") {
        return e.toUpperCase() // works, `e` narrowed to string
    } else if (e instanceof Error) {
        return e.message // works, `e` narrowed to Error
    }
    return
}

export function isValidDate(date: any) {
    return date && Object.prototype.toString.call(date) === "[object Date]" && !isNaN(date)
}

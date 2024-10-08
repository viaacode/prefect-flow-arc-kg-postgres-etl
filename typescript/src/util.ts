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

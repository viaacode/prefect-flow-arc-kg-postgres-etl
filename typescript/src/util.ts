import { DEBUG_MODE, LOGGING_LEVEL } from './configuration.js'
import { Stats } from './stats.js'

// Global stats object to keep track of progress
export const stats = new Stats()

// Logging methods to communicate with Prefect
export function logInfo(message: any, ...optionalParams: any[]) {
    logPrefect('INFO', message, optionalParams)
}

export function logError(message: any, error?: unknown, ...optionalParams: any[]) {
    logPrefect('ERROR', message + ':' + getErrorMessage(error), optionalParams)
}

export function logDebug(message: any, ...optionalParams: any[]) {
    if (LOGGING_LEVEL === 'DEBUG' || DEBUG_MODE)
        logPrefect('DEBUG', message, optionalParams)
}

export function logWarning(message: any, ...optionalParams: any[]) {
    if (LOGGING_LEVEL !== 'ERROR')
        logPrefect('WARNING', message, optionalParams)
}

export function logPrefect(level: "INFO" | "ERROR" | "DEBUG" | "WARNING", message: any, ...optionalParams: any[]) {
    if (LOGGING_LEVEL)
        process.stdout.write(JSON.stringify({ PREFECT: { time: new Date().toISOString(), level, message, progress: stats.progress, stats, context: optionalParams } }) + '\n')
}

// Helper function to get the error message in typescript
export function getErrorMessage(e: unknown): string | undefined {
    if (typeof e === "string") {
        return e.toString() // works, `e` narrowed to string
    } else if (e instanceof Error) {
        return e.message // works, `e` narrowed to Error
    } else if (typeof e === 'object' && e !== null && 'message' in e) {
        return (e as { message?: unknown }).message?.toString();
    }
    return "Unknown error";
}

// Helper function to check if a date is a Date object
export function isValidDate(date: any) {
    return date && Object.prototype.toString.call(date) === "[object Date]" && !isNaN(date)
}

// Format milliseconds as a formatted string
export function msToTime(s: number) {
    s = Math.round(s)
    // Pad to 2 or 3 digits, default is 2
    var pad = (n: number, z = 2) => ('00' + n).slice(-z)
    return pad(s / 3.6e6 | 0) + ':' + pad((s % 3.6e6) / 6e4 | 0) + ':' + pad((s % 6e4) / 1000 | 0) + '.' + pad(s % 1000, 3)
}

import memwatch from '@airbnb/node-memwatch'
import { logDebug, logInfo } from './util.js'
import { DEBUG_MODE } from './configuration.js'
let heapDiff: memwatch.HeapDiff

if (DEBUG_MODE) {
    logInfo('Debug mode is on.')
    heapDiff = new memwatch.HeapDiff();
    setInterval(logHeap, 5*60*1000)
}

function logHeap() {
    logDebug('Heap difference',heapDiff.end());
    heapDiff = new memwatch.HeapDiff();
}
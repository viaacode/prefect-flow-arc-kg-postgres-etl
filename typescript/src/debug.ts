import memwatch from '@airbnb/node-memwatch'
import { logDebug } from './util.js'
import { DEBUG_MODE } from './configuration.js'
let heapDiff = new memwatch.HeapDiff();

if (DEBUG_MODE) {
    setInterval(logHeap, 5*60*1000)
}

function logHeap() {
    logDebug(JSON.stringify(heapDiff.end()));
    heapDiff = new memwatch.HeapDiff();
}
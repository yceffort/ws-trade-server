import { parentPort, workerData } from 'node:worker_threads'
import { probe } from './run.mjs'
try { parentPort.postMessage(await probe(workerData)) }
catch { parentPort.postMessage({ ok: false, stage: 'connect', samples: {}, attempts: 0 }) }

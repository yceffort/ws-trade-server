import { parentPort, workerData } from 'node:worker_threads'
import { probe } from './run.js'
const started = performance.now()
try {
  parentPort.postMessage(await probe(workerData))
} catch {
  parentPort.postMessage({
    ok: false,
    target: workerData.target,
    protocol: workerData.protocol,
    stage: null,
    samples: {},
    attempts: 0,
    elapsed_seconds: (performance.now() - started) / 1000,
    worker_failed: true
  })
}

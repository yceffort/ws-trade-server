import { Worker } from 'node:worker_threads'
import { createServer } from 'node:http'
import { pathToFileURL } from 'node:url'
import { readBounded } from '../examples/ha-state.mjs'
import { validate, stages } from './run.mjs'

const bounds = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]
export class Metrics {
  constructor(targets) {
    this.targets = targets.map(config => ({ config, running: false, failures: 0, result: null, last: null, due: Date.now() / 1000 + config.deadlineMs / 1000 + 5, histograms: new Map() }))
  }
  record(target, result) {
    target.result = result; target.last = Date.now() / 1000
    // 실패한 실행의 부분 표본은 성공 지연 분포에 섞지 않는다.
    if (!result.ok) return
    for (const [stage, value] of Object.entries(result.samples)) {
      if (!stages.includes(stage) || !Number.isFinite(value) || value < 0) continue
      const h = target.histograms.get(stage) ?? { buckets: bounds.map(() => 0), count: 0, sum: 0 }
      h.count++; h.sum += value
      bounds.forEach((bound, i) => { if (value <= bound) h.buckets[i]++ })
      target.histograms.set(stage, h)
    }
  }
  text() {
    const lines = []
    for (const target of this.targets) {
      const labels = `target="${target.config.target}",protocol="${target.config.protocol}"`
      lines.push(`wts_probe_next_deadline_seconds{${labels}} ${target.due}`, `wts_probe_running{${labels}} ${Number(target.running)}`, `wts_probe_worker_failures_total{${labels}} ${target.failures}`)
      if (target.last !== null) lines.push(`wts_probe_last_run_seconds{${labels}} ${target.last}`, `wts_probe_success{${labels}} ${Number(target.result.ok)}`)
      for (const stage of stages) {
        if (stages.includes(target.result?.stage)) lines.push(`wts_probe_failed_stage{${labels},stage="${stage}"} ${Number(!target.result.ok && target.result.stage === stage)}`)
        const h = target.histograms.get(stage)
        if (!h) continue
        const scoped = `${labels},stage="${stage}"`
        bounds.forEach((bound, i) => lines.push(`wts_probe_stage_seconds_bucket{${scoped},le="${bound}"} ${h.buckets[i]}`))
        lines.push(`wts_probe_stage_seconds_bucket{${scoped},le="+Inf"} ${h.count}`, `wts_probe_stage_seconds_count{${scoped}} ${h.count}`, `wts_probe_stage_seconds_sum{${scoped}} ${h.sum}`)
      }
    }
    return lines.join('\n') + '\n'
  }
}

export function start(config) {
  if (!Array.isArray(config.targets) || !config.targets.length || config.targets.length > 8 || new Set(config.targets.map(t => t.target)).size !== config.targets.length || !Number.isInteger(config.intervalMs) || config.intervalMs < 1000 || config.intervalMs > 300000) throw new Error('invalid-config')
  config.targets.forEach(validate)
  const metrics = new Metrics(config.targets), workers = new Set(), timers = new Set()
  let stopped = false
  const run = async target => {
    target.running = true
    const worker = new Worker(new URL('./worker.mjs', import.meta.url), { workerData: target.config, resourceLimits: { maxOldGenerationSizeMb: 64, maxYoungGenerationSizeMb: 16 } })
    workers.add(worker)
    let result
    const timer = setTimeout(() => { worker.terminate() }, target.config.deadlineMs + 1000)
    worker.on('message', message => { result = message })
    worker.on('error', () => {})
    worker.once('exit', () => {
      clearTimeout(timer); workers.delete(worker); target.running = false
      if (stopped) return
      if (!result || result.worker_failed) { target.failures++; result = { ok: false, stage: null, samples: {} } }
      metrics.record(target, result)
      target.due = Date.now() / 1000 + (config.intervalMs + target.config.deadlineMs) / 1000 + 5
      console.log(JSON.stringify({ event: 'probe_result', target: target.config.target, protocol: target.config.protocol, ok: result.ok, stage: result.stage }))
      const next = setTimeout(() => { timers.delete(next); run(target) }, config.intervalMs)
      timers.add(next)
    })
  }
  const server = createServer((request, response) => {
    if (request.url !== '/metrics') { response.writeHead(404).end(); return }
    response.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' }).end(metrics.text())
  })
  server.maxConnections = 16; server.requestTimeout = 5000; server.headersTimeout = 5000
  server.listen(config.port ?? 9464, config.host ?? '127.0.0.1')
  for (const target of metrics.targets) run(target)
  return { server, metrics, async close() { stopped = true; for (const timer of timers) clearTimeout(timer); await Promise.all([...workers].map(worker => worker.terminate())); server.close(); server.closeAllConnections() } }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('TLS verification required')
    const processHandle = start(JSON.parse(readBounded(process.argv[2], 64 * 1024)))
    for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => processHandle.close())
  } catch { console.error('probe configuration failed'); process.exitCode = 1 }
}

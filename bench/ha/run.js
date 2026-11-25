import { Worker } from 'node:worker_threads'
import { createServer } from 'node:http'
import { mkdirSync, readFileSync, writeFileSync, existsSync, chmodSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { randomUUID } from 'node:crypto'
import { performance } from 'node:perf_hooks'
import { compare, percentile, reconnectMetrics } from './oracle.js'
import { measuredTargets } from './measurement.js'
import { experiment, loadSample } from './observability.js'
const exec = promisify(execFile)
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
const workers = [],
  exits = [],
  requests = new Map(),
  ready = new Set(),
  failures = [],
  verifications = [],
  actions = []
const windows = new Map()
let server,
  config,
  stopped = false,
  activeScope = null
async function mark(scope) {
  const previous = activeScope
  if (scope) activeScope = scope
  const id = randomUUID()
  await new Promise((resolve, reject) => {
    let replies = 0
    const timer = setTimeout(() => {
      requests.delete(id)
      reject(new Error('measurement mark deadline'))
    }, 5000)
    requests.set(id, () => {
      if (++replies === workers.length) {
        clearTimeout(timer)
        requests.delete(id)
        resolve()
      }
    })
    workers.forEach(worker => worker.postMessage({ type: 'mark', scope, id }))
  })
  // 늦게 끝난 해제가 그 사이에 시작된 다른 범위를 지우지 않게 한다.
  if (!scope && activeScope === previous) activeScope = null
  const event = experiment(scope ?? previous, scope ? 'start' : 'finish')
  if (event) console.log(JSON.stringify(event))
}
function windowKey(request) {
  if (
    typeof request.window !== 'string' ||
    !request.window ||
    request.window.length > 64 ||
    typeof request.planHash !== 'string' ||
    !['wts-gateway-a', 'wts-gateway-b', 'wts-gateway-c'].includes(request.slot) ||
    typeof request.rollback !== 'boolean'
  )
    throw new Error('invalid measurement window')
  return JSON.stringify([request.planHash, request.slot, request.rollback])
}
async function begin(request) {
  const key = windowKey(request)
  if (activeScope && (activeScope.name || activeScope.window !== request.window))
    throw new Error('another measurement is active')
  if (windows.has(request.window) && windows.get(request.window) !== key)
    throw new Error('measurement window identity mismatch')
  if (!windows.has(request.window) && windows.size >= 64) throw new Error('measurement window limit')
  if (ready.size !== config.clients || failures.length)
    throw new Error('all clients must be caught up before measurement')
  windows.set(request.window, key)
  await mark({ window: request.window, rollback: request.rollback })
  return { ...request, ok: true }
}
async function cancel(request) {
  const key = windowKey(request),
    known = windows.get(request.window)
  if (known !== undefined && known !== key) throw new Error('measurement window identity mismatch')
  if (known === key && activeScope?.window === request.window) await mark(null)
  return { ...request, ok: true }
}
async function command(argv, extra = [], timeout = 90000) {
  if (!Array.isArray(argv) || !argv.length || argv.some(s => typeof s !== 'string'))
    throw new Error('command argv required')
  const { stdout } = await exec(argv[0], [...argv.slice(1), ...extra], { timeout, maxBuffer: 128 * 1024 * 1024 })
  return JSON.parse(stdout)
}
async function oracleCommand(argv, extra, deadline) {
  for (;;) {
    const remaining = Math.floor(deadline - performance.now())
    if (remaining <= 0) throw new Error('oracle recovery deadline')
    try {
      return await command(argv, extra, remaining)
    } catch (error) {
      if (error instanceof SyntaxError) throw error
      if (performance.now() >= deadline) throw new Error('oracle recovery deadline')
      await delay(250)
    }
  }
}
async function sample() {
  const id = randomUUID()
  return await new Promise((resolve, reject) => {
    const replies = []
    const timer = setTimeout(() => {
      requests.delete(id)
      reject(new Error('client worker sample deadline'))
    }, 5000)
    requests.set(id, reply => {
      replies.push(reply)
      if (replies.length === workers.length) {
        clearTimeout(timer)
        requests.delete(id)
        resolve({
          clients: replies.flatMap(r => r.clients),
          samples: replies.flatMap(r => r.samples),
          disconnect_reasons: replies.reduce((total, r) => {
            for (const [reason, count] of Object.entries(r.reasons)) total[reason] = (total[reason] ?? 0) + count
            return total
          }, {}),
          client_event_loop_p99_ms: replies.every(r => r.lag_p99_ms !== null)
            ? Math.max(...replies.map(r => r.lag_p99_ms))
            : null,
          client_event_loop_max_ms: Math.max(...replies.map(r => r.lag_max_ms))
        })
      }
    })
    workers.forEach(worker => worker.postMessage({ type: 'sample', id }))
  })
}
async function status() {
  const observed = await sample(),
    offsets = observed.clients.filter(c => c.offset !== null).map(c => BigInt(c.offset))
  const reasons = {}
  for (const client of observed.clients)
    if (client.last_reason) reasons[client.last_reason] = (reasons[client.last_reason] ?? 0) + 1
  return {
    clients: observed.clients.length,
    caught_up: observed.clients.filter(c => c.caught_up).length,
    failures: failures.length,
    minimum_offset: offsets.length ? String(offsets.reduce((a, b) => (a < b ? a : b))) : null,
    maximum_offset: offsets.length ? String(offsets.reduce((a, b) => (a > b ? a : b))) : null,
    reasons,
    disconnect_reasons: observed.disconnect_reasons,
    active_measurement: activeScope ? (activeScope.name ? 'fault' : 'rollout') : null,
    completed_actions: actions.filter(a => a.recovery?.ok).map(a => a.name),
    client_event_loop_p99_ms: observed.client_event_loop_p99_ms,
    client_event_loop_max_ms: observed.client_event_loop_max_ms
  }
}
function requireWindow(request) {
  if (request.window !== undefined && windows.get(request.window) !== windowKey(request))
    throw new Error('measurement window was not started')
}
async function verify(request = {}, requireSlo = config.requireReconnectSlo) {
  requireWindow(request)
  const deadline = performance.now() + config.recoveryDeadlineMs
  const boundary = await oracleCommand(config.headCommand, [], deadline)
  const end = BigInt(boundary.next) - 1n
  let observed
  do {
    observed = await sample()
    if (
      observed.clients.length === config.clients &&
      observed.clients.every(c => !c.failed && c.caught_up && c.offset !== null && BigInt(c.offset) >= end)
    )
      break
    await delay(100)
  } while (performance.now() < deadline)
  if (observed.clients.some(c => c.failed || !c.caught_up || c.offset === null || BigInt(c.offset) < end))
    throw new Error('clients did not apply the committed verification boundary')
  const maximum = observed.clients.reduce((n, c) => (BigInt(c.offset) > n ? BigInt(c.offset) : n), 0n)
  const oracle = await oracleCommand(config.rangeCommand, [config.start, maximum.toString()], deadline)
  const comparison = compare(oracle, observed.clients)
  const samples = request.fault ? observed.samples.filter(s => s.fault === request.fault) : observed.samples
  const reconnect = reconnectMetrics(samples, request.window)
  const recovery = percentile(
    samples.filter(s => s.fault_ms !== null).map(s => s.fault_ms),
    0.99
  )
  const slo = reconnect.reconnect_slo_status === 'pass'
  const result = {
    ...request,
    ok: comparison.ok && (!requireSlo || slo),
    committed_missing: comparison.ok ? 0 : null,
    duplicate_applies: comparison.ok ? 0 : null,
    comparison,
    ...reconnect,
    recovery_from_fault_p99_ms: recovery,
    disconnect_reasons: observed.disconnect_reasons,
    client_event_loop_p99_ms: observed.client_event_loop_p99_ms,
    client_event_loop_max_ms: observed.client_event_loop_max_ms,
    log_id: config.logId,
    range_start: config.start,
    verified_through: end.toString()
  }
  return result
}
try {
  config = JSON.parse(readFileSync(process.argv[2]))
  config.clients ??= 1000
  config.workers ??= 4
  config.durationSeconds ??= 600
  config.recoveryDeadlineMs ??= 60000
  config.requireReconnectSlo ??= true
  config.startupIntervalMs ??= 10
  if (
    !Number.isInteger(config.clients) ||
    config.clients < 1 ||
    config.clients > 10000 ||
    !Number.isInteger(config.workers) ||
    config.workers < 1 ||
    config.workers > 16 ||
    config.durationSeconds < 10 ||
    config.durationSeconds > 900
  )
    throw new Error('invalid load limits')
  if (
    !Number.isInteger(config.recoveryDeadlineMs) ||
    config.recoveryDeadlineMs < 1 ||
    config.recoveryDeadlineMs > 60000
  )
    throw new Error('invalid recovery deadline')
  if (!config.stateDirectory || existsSync(config.stateDirectory))
    throw new Error('fresh durable state directory required')
  config.stateDirectory = resolve(config.stateDirectory)
  mkdirSync(config.stateDirectory, { recursive: true, mode: 0o700 })
  chmodSync(config.stateDirectory, 0o700)
  config.tokenFile = resolve(config.tokenFile)
  const head = await command(config.headCommand)
  config.start = head.next
  config.logId = head.log_id
  config.topics ??= head.topics
  if (!Array.isArray(config.urls) || !config.urls.length) throw new Error('HA endpoints required')
  for (const value of config.urls) {
    const url = new URL(value)
    if (
      (url.protocol !== 'wss:' &&
        !(url.protocol === 'ws:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname))) ||
      url.pathname !== '/experimental/ha/ws' ||
      url.username ||
      url.password ||
      url.search
    )
      throw new Error('invalid HA endpoint')
  }
  for (let index = 0; index < Math.min(config.workers, config.clients); index++) {
    const ids = Array.from({ length: config.clients }, (_, i) => i).filter(i => i % config.workers === index)
    const worker = new Worker(new URL('./worker.js', import.meta.url), {
      workerData: { ids, config, head },
      resourceLimits: { maxOldGenerationSizeMb: 192, maxYoungGenerationSizeMb: 32 }
    })
    workers.push(worker)
    exits.push(new Promise(resolve => worker.once('exit', resolve)))
    worker.on('message', message => {
      if (message.type === 'ready') ready.add(message.client)
      if (message.type === 'not-ready') ready.delete(message.client)
      if (message.type === 'failed') failures.push(message)
      if (message.type === 'sample' || message.type === 'marked') requests.get(message.id)?.(message)
    })
    worker.on('error', () => failures.push({ reason: 'worker-error' }))
    worker.on('exit', code => {
      if (code !== 0 && !stopped) failures.push({ reason: 'worker-exit' })
    })
  }
  // 마지막 클라이언트는 worker.js에서 (clients - 1) * startupIntervalMs 뒤에야 시작한다.
  const startup = performance.now() + (config.clients - 1) * config.startupIntervalMs + 60000
  while (ready.size < config.clients && !failures.length && performance.now() < startup) await delay(100)
  if (ready.size !== config.clients || failures.length) throw new Error('initial client catchup failed')
  const started = performance.now()
  const observe = () => {
    if (stopped) return
    sample()
      .then(value => console.log(JSON.stringify(loadSample(value))))
      .catch(() => console.log(JSON.stringify(loadSample({ client_event_loop_p99_ms: null }))))
      .finally(() => {
        if (!stopped) setTimeout(observe, 5000)
      })
  }
  observe()
  if (config.controlSocket) {
    config.controlSocket = resolve(config.controlSocket)
    if (existsSync(config.controlSocket)) throw new Error('control socket already exists')
    server = createServer(async (request, response) => {
      try {
        if (request.method === 'GET' && request.url === '/status') {
          response.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(await status()))
          return
        }
        if (request.method !== 'POST' || !['/begin', '/verify', '/cancel'].includes(request.url))
          throw new Error('unknown control operation')
        let text = ''
        for await (const chunk of request) {
          text += chunk
          if (text.length > 16384) throw new Error('control input limit')
        }
        const input = JSON.parse(text)
        let result
        if (request.url === '/begin') result = await begin(input)
        else if (request.url === '/cancel') result = await cancel(input)
        else {
          requireWindow(input)
          try {
            result = await verify(input)
          } finally {
            if (activeScope?.window === input.window && input.window !== undefined) await mark(null)
          }
        }
        if (request.url === '/verify') {
          verifications.push({
            slot: result.slot,
            planHash: result.planHash,
            window: result.window,
            rollback: result.rollback,
            ok: result.ok,
            reconnect_p99_ms: result.reconnect_p99_ms,
            reconnect_slo_status: result.reconnect_slo_status,
            verified_through: result.verified_through
          })
        }
        response.writeHead(result.ok ? 200 : 409, { 'content-type': 'application/json' }).end(JSON.stringify(result))
      } catch (error) {
        response.writeHead(409).end(
          JSON.stringify({
            ok: false,
            reason: error.code
              ? 'external-command-or-io-failed'
              : error instanceof SyntaxError
                ? 'invalid-json'
                : error.message
          })
        )
      }
    })
    server.maxConnections = 1
    server.requestTimeout = 10000
    server.headersTimeout = 5000
    server.keepAliveTimeout = 1000
    await new Promise(resolve => server.listen(config.controlSocket, resolve))
    chmodSync(config.controlSocket, 0o600)
  }
  const signal = () => {
    stopped = true
  }
  process.once('SIGTERM', signal)
  process.once('SIGINT', signal)
  for (const action of config.actions ?? []) {
    if (
      !action.name ||
      !Number.isFinite(action.atSeconds) ||
      action.atSeconds < 0 ||
      action.atSeconds >= config.durationSeconds
    )
      throw new Error('invalid fault schedule')
  }
  for (const action of [...(config.actions ?? [])].sort((a, b) => a.atSeconds - b.atSeconds)) {
    while (!stopped && !failures.length && performance.now() - started < action.atSeconds * 1000) await delay(100)
    if (stopped || failures.length) break
    if (activeScope) throw new Error('scheduled fault overlaps rollout measurement')
    const fault = randomUUID()
    await mark({ name: action.name, fault })
    const at = performance.now()
    let result
    try {
      result = await command(action.command, [], 180000)
    } catch (error) {
      actions.push({ name: action.name, fault, elapsed_ms: performance.now() - at, result: { ok: false } })
      throw error
    }
    actions.push({ name: action.name, fault, elapsed_ms: performance.now() - at, result })
    if (result.ok !== true) throw new Error('fault action failed')
    const recovery = await verify({ fault }, false)
    if (!recovery.ok) throw new Error('fault recovery application mismatch')
    actions.at(-1).recovery = recovery
    await mark(null)
  }
  while (!stopped && !failures.length && performance.now() - started < config.durationSeconds * 1000) await delay(100)
  const report = await verify()
  const elapsed = performance.now() - started
  report.ok &&= failures.length === 0
  report.duration_ms = elapsed
  report.clients = config.clients
  report.actions = actions
  report.rollout_verifications = verifications
  report.failures = failures
  report.environment = config.environment ?? { kind: 'unspecified' }
  report.measured_targets_met = measuredTargets(report)
  report.production_slo_proven = false
  report.external_evidence_review_required = true
  mkdirSync(dirname(config.output), { recursive: true })
  writeFileSync(config.output, JSON.stringify(report, null, 2), { mode: 0o600 })
  console.log(
    JSON.stringify({ ok: report.ok, report: config.output, production_slo_proven: report.production_slo_proven })
  )
  if (!report.ok) process.exitCode = 1
} catch (error) {
  const reason = error.code ? 'external-command-or-io-failed' : error.message
  if (config?.output) {
    const diagnostics = workers.length ? await status().catch(() => ({ unavailable: true })) : null
    mkdirSync(dirname(config.output), { recursive: true })
    writeFileSync(
      config.output,
      JSON.stringify(
        {
          ok: false,
          reason,
          clients: config.clients,
          ready: ready.size,
          log_id: config.logId,
          range_start: config.start,
          failures,
          diagnostics,
          actions,
          rollout_verifications: verifications,
          production_slo_proven: false
        },
        null,
        2
      ),
      { mode: 0o600 }
    )
  }
  console.error(reason)
  process.exitCode = 1
} finally {
  stopped = true
  for (const worker of workers) worker.postMessage({ type: 'stop' })
  // 이 실행이 연 제어 소켓 파일은 close가 지운다. 경로로 지우면 다른 실행의 소켓까지 지운다.
  server?.close()
  server?.closeAllConnections()
  await Promise.all(
    workers.map(async (worker, index) => {
      const timer = setTimeout(() => worker.terminate(), 5000)
      await exits[index]
      clearTimeout(timer)
    })
  )
}

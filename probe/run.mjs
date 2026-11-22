import { performance } from 'node:perf_hooks'
import { promisify } from 'node:util'
import { execFile } from 'node:child_process'
import { Client, topics, until } from '../bench/soak/client.mjs'
import { LatestState } from '../examples/latest-state.mjs'
import { runClient } from '../examples/ha-client.mjs'
import { MemoryStore, readBounded } from '../examples/ha-state.mjs'
import { compare, extend, payloadHash, zero } from '../bench/ha/oracle.mjs'

const exec = promisify(execFile)
export const stages = ['connect', 'tls', 'auth', 'subscribe', 'first_event', 'apply', 'reconnect', 'catch_up', 'oracle']
export function validate(config) {
  if (!/^[a-z][a-z0-9_-]{0,31}$/.test(config.target) || !['stomp', 'ha'].includes(config.protocol)) throw new Error('invalid-config')
  const url = new URL(config.url)
  if (url.protocol !== 'wss:' && !(url.protocol === 'ws:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) || url.pathname !== (config.protocol === 'ha' ? '/experimental/ha/ws' : '/ws') || url.username || url.password || url.search || url.hash) throw new Error('invalid-config')
  if (!Number.isInteger(config.deadlineMs) || config.deadlineMs < 100 || config.deadlineMs > 60000) throw new Error('invalid-config')
  if (config.protocol === 'ha' && (!Array.isArray(config.topics) || !config.topics.length || config.topics.length > 4 || config.topics.some(t => typeof t !== 'string' || !t || t.length > 256) || new Set(config.topics).size !== config.topics.length)) throw new Error('invalid-config')
  return config
}

export async function probe(config, dependencies = {}) {
  validate(config)
  const controller = new AbortController(), started = performance.now(), samples = {}
  const deadline = AbortSignal.timeout(config.deadlineMs)
  const signal = AbortSignal.any([controller.signal, deadline])
  let stage = 'connect', failure, attempt = 0
  const enter = value => { stage = value }
  const sample = (name, at) => { samples[name] = (performance.now() - at) / 1000 }
  const token = dependencies.token ?? (() => readBounded(config.tokenFile, 16384).toString('utf8').trim())
  const ca = dependencies.ca ?? (config.caFile ? readBounded(config.caFile, 1024 * 1024) : undefined)
  const command = dependencies.command ?? (async (argv, extra = []) => {
    if (!Array.isArray(argv) || !argv.length || argv.some(v => typeof v !== 'string')) throw new Error('oracle-command')
    const { stdout } = await exec(argv[0], [...argv.slice(1), ...extra], { signal: deadline, timeout: Math.max(1, config.deadlineMs - (performance.now() - started)), maxBuffer: 4 * 1024 * 1024 })
    return JSON.parse(stdout)
  })
  const connection = socket => {
    if (++attempt > 3) { failure = stage; controller.abort(); socket.terminate(); return }
    const at = performance.now()
    enter(attempt > 1 ? 'reconnect' : config.url.startsWith('wss:') ? 'tls' : 'connect')
    socket.once('open', () => {
      if (socket.protocol !== (config.protocol === 'ha' ? 'wts.ha.v1' : 'v12.stomp')) { failure = 'connect'; controller.abort(); socket.terminate(); return }
      sample(attempt > 1 ? 'reconnect' : 'connect', at)
      if (config.url.startsWith('wss:')) sample('tls', at)
      enter('auth')
    })
  }
  try {
    if (config.protocol === 'stomp') {
      const state = new LatestState()
      for (let cycle = 0; cycle < 2; cycle++) {
        signal.throwIfAborted()
        const credential = await token()
        if (!credential || /[\r\n\0]/.test(credential)) throw new Error('invalid-token')
        const client = new Client(config.url, credential.replace(/^Bearer /, ''), ca, signal)
        connection(client.ws)
        const stop = () => client.terminate()
        signal.addEventListener('abort', stop, { once: true })
        let applied = 0, firstAt
        client.ws.on('message', (bytes, binary) => {
          if (binary || client.failure || client.serverError) return
          const text = bytes.toString(), split = text.indexOf('\n\n')
          if (!text.startsWith('MESSAGE\n')) return
          try {
            const topic = /^subscription:(.+)$/m.exec(text.slice(0, split))?.[1]
            if (!topics.includes(topic) || !client.receipts.has(topic)) throw new Error('unexpected-topic')
            const body = JSON.parse(text.slice(split + 2, -1))
            if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('invalid-body')
            firstAt ??= performance.now(); state.stream(topic, body); applied++
          } catch { failure = 'apply'; stop() }
        })
        try {
          let at = performance.now()
          await until(() => { client.healthy(); return client.connected }, 'auth', signal, config.deadlineMs)
          sample('auth', at); enter('subscribe'); at = performance.now()
          for (const topic of topics) client.send('SUBSCRIBE', { id: topic, destination: topic, receipt: topic })
          await until(() => { client.healthy(); return topics.every(topic => client.receipts.has(topic)) }, 'subscribe', signal, config.deadlineMs)
          sample('subscribe', at); enter('first_event'); at = performance.now()
          await until(() => { client.healthy(); return applied > 0 && topics.every(topic => client.topicMessages[topic] > 0) }, 'first event', signal, config.deadlineMs)
          samples.first_event = Math.max(0, (firstAt - at) / 1000)
          enter('apply'); sample('apply', firstAt)
          if (state.values.size !== topics.length) throw new Error('state-incomplete')
        } finally { signal.removeEventListener('abort', stop); client.terminate() }
      }
    } else {
      enter('oracle')
      const head = await command(config.headCommand)
      if (typeof head.log_id !== 'string' || !/^(0|[1-9]\d*)$/.test(head.next) || !config.topics.every(topic => head.topics?.includes(topic))) throw new Error('invalid-oracle-head')
      const store = new MemoryStore('events', [...config.topics].sort())
      let pending = new Map(), bytes = 0, payloadDigest = zero, firstCount = null, caught = false, secondCaught = false, socket, attemptAt = 0
      await runClient({ urls: [config.url], token, ca, store, startAt: { log_id: head.log_id, partition: 0, offset: head.next }, signal,
        onAttempt(ws) { socket = ws; pending = new Map(); bytes = 0; caught = false; attemptAt = performance.now(); connection(ws) },
        onDisconnect(reason) { if (!controller.signal.aborted && ['auth-invalid', 'auth-expired', 'auth-revoked', 'forbidden-scope'].includes(reason)) failure = 'auth' },
        onFrame(frame) {
          if (frame.type === 'connected') { sample('auth', attemptAt); enter('subscribe') }
          if (frame.type === 'event') {
            sample('first_event', attemptAt); enter('apply')
            if (pending.size >= 10000 || (bytes += Buffer.byteLength(JSON.stringify(frame))) > 1024 * 1024) throw new Error('probe-memory-limit')
            const hash = payloadHash(frame.body)
            if (pending.has(frame.event_id) && pending.get(frame.event_id) !== hash) throw new Error('changed-event')
            pending.set(frame.event_id, hash)
          }
          if (frame.type === 'caught-up') { sample('catch_up', attemptAt); caught = true; if (attempt > 1) secondCaught = true; progress() }
        },
        onCommit(state, ids) {
          for (const id of ids) {
            if (!pending.has(id)) throw new Error('missing-payload')
            payloadDigest = extend(payloadDigest, id, pending.get(id))
          }
          pending.clear(); bytes = 0
          if (ids.length) sample('apply', attemptAt)
          progress()
        },
      })
      function progress() {
        const count = BigInt(store.load().count)
        if (caught && count > 0n && firstCount === null) { firstCount = count; enter('reconnect'); socket.terminate() }
        else if (secondCaught && count > firstCount) controller.abort()
        else if (caught) enter('first_event')
      }
      if (failure || !secondCaught || firstCount === null || BigInt(store.load().count) <= firstCount) throw new Error('incomplete-probe')
      // 클라이언트 중지는 내부 controller만 멈춘다. oracle도 같은 전체 실행 기한 안에서 읽는다.
      enter('oracle')
      const at = performance.now(), state = store.load()
      const oracle = await command(config.rangeCommand, [head.next, state.offset])
      if (oracle.start !== head.next || oracle.through !== state.offset || oracle.log_id !== head.log_id || oracle.events.length > 10000 || !compare(oracle, [{ id: 0, ...state, payload_digest: payloadDigest }]).ok) throw new Error('oracle-mismatch')
      sample('oracle', at)
    }
    return { ok: true, target: config.target, protocol: config.protocol, stage: 'apply', samples, attempts: attempt, elapsed_seconds: (performance.now() - started) / 1000 }
  } catch {
    return { ok: false, target: config.target, protocol: config.protocol, stage: failure ?? stage, samples, attempts: attempt, elapsed_seconds: (performance.now() - started) / 1000 }
  } finally { controller.abort() }
}

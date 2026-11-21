import { pathToFileURL } from 'node:url'
import { Application, FileStore, readBounded } from './ha-state.mjs'

const terminal = new Set(['cursor-expired', 'invalid-cursor', 'invalid-resume', 'start-out-of-range', 'forbidden-scope', 'auth-invalid', 'auth-expired', 'auth-revoked'])

export async function runClient({ urls, token, store, startAt, signal, onCommit = () => {}, onFrame = () => {} }) {
  const { default: WebSocket } = await import('../edge/node_modules/ws/wrapper.mjs')
  let failures = 0, noProgress = 0, attempt = 0
  while (!signal?.aborted) {
    const before = store.load()
    const application = new Application(store)
    const socket = new WebSocket(urls[attempt++ % urls.length], 'wts.ha.v1', { handshakeTimeout: 3000, maxPayload: 48 * 1024 })
    let queued = 0, chain = Promise.resolve(), reason = 'connection-closed', ready = false, applyFailed = false
    const closed = new Promise(resolve => socket.once('close', resolve))
    const stop = () => socket.terminate()
    signal?.addEventListener('abort', stop, { once: true })
    let receiveTimer = setTimeout(() => { reason = 'receive-deadline'; stop() },3000)
    const timer = setInterval(() => {
      if (ready && socket.readyState === WebSocket.OPEN) socket.send('{"type":"heartbeat"}')
    }, 1000)
    socket.on('error', () => { reason = 'connection-error' })
    socket.once('open', async () => {
      try { socket.send(JSON.stringify({ type: 'connect', token: await token() })) }
      catch { reason = 'auth-invalid'; stop() }
    })
    socket.on('message', (data, binary) => {
      clearTimeout(receiveTimer)
      receiveTimer = setTimeout(() => { reason = 'receive-deadline'; stop() },3000)
      queued += data.length
      if (binary || queued > 1024 * 1024) { reason = 'client-overflow'; return stop() }
      chain = chain.then(async () => {
        if (applyFailed) return
        const frame = JSON.parse(data.toString())
        await onFrame(frame, socket)
        if (!ready && !['connected', 'error', 'heartbeat'].includes(frame.type)) throw new Error('connect-required')
        if (frame.type === 'connected') {
          if (ready) throw new Error('duplicate-connected')
          const saved = store.load()
          const resume = { type: 'resume', request_id: 'resume', topics: saved.topics, mode: saved.mode }
          if (saved.cursor) resume.cursor = saved.cursor
          else if (saved.mode === 'events') resume.start_at = startAt
          socket.send(JSON.stringify(resume)); ready = true
        } else if (frame.type === 'error') {
          reason = typeof frame.code === 'string' ? frame.code : 'invalid-response'
          stop()
        } else if (frame.type === 'draining') {
          reason = 'draining'; stop()
        } else {
          const ids = application.receive(frame)
          if (frame.type === 'checkpoint') await onCommit(store.load(), ids)
        }
      }).catch(error => { applyFailed = true; reason = error.message === 'auth-invalid' ? error.message : 'client-apply-failed'; stop() }).finally(() => { queued -= data.length })
    })
    await closed
    clearInterval(timer)
    clearTimeout(receiveTimer)
    signal?.removeEventListener('abort', stop)
    await chain
    if (signal?.aborted) return store.load()
    const after = store.load()
    const progressed = after.offset !== null && (before.offset === null || BigInt(after.offset) > BigInt(before.offset))
    if (terminal.has(reason) || reason === 'client-apply-failed') throw new Error(reason)
    if (!progressed && (!after.cursor || reason === 'replay-limit')) {
      if (++noProgress >= 3) throw new Error('no-checkpoint-progress')
    } else noProgress = 0
    failures = progressed ? 0 : Math.min(failures + 1, 6)
    const delay = attempt === 1 ? Math.random() * 500 : Math.random() * Math.min(30000, 500 * 2 ** failures)
    await new Promise(resolve => {
      const done = () => { clearTimeout(timeout); signal?.removeEventListener('abort', done); resolve() }
      const timeout = setTimeout(done, delay)
      signal?.addEventListener('abort', done, { once: true })
    })
  }
  return store.load()
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const args = Object.fromEntries(Array.from({ length: (process.argv.length - 2) / 2 }, (_, i) => [process.argv[i * 2 + 2], process.argv[i * 2 + 3]]))
    const topics = args['--topics'].split(',').sort()
    const mode = args['--mode'] ?? 'state'
    if (!['state', 'events'].includes(mode) || !args['--state'] || !args['--token-file']) throw new Error('invalid-options')
    const urls = args['--urls'].split(',')
    for (const endpoint of urls) {
      const url = new URL(endpoint)
      if (url.pathname !== '/experimental/ha/ws' || url.search || url.username || url.password || url.protocol !== 'wss:' && !(url.protocol === 'ws:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))) throw new Error('invalid-endpoint')
    }
    const controller = new AbortController()
    for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => controller.abort())
    const state = await runClient({ urls, token: () => `Bearer ${readBounded(args['--token-file'], 16384).toString('utf8').trim()}`, store: new FileStore(args['--state'], mode, topics), startAt: mode === 'events' ? { log_id: args['--log-id'], partition: 0, offset: args['--start-at'] } : undefined, signal: controller.signal })
    console.log(JSON.stringify({ applied: state.count, offset: state.offset, digest: state.digest }))
  } catch {
    console.error('HA 클라이언트가 복구를 완료하지 못했습니다. 설정, 권한과 저장 위치를 확인하세요.')
    process.exitCode = 1
  }
}

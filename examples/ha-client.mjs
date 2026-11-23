import { pathToFileURL } from 'node:url'
import { Application, FileStore, readBounded } from './ha-state.mjs'

const terminal = new Set(['cursor-expired', 'invalid-cursor', 'invalid-resume', 'start-out-of-range', 'forbidden-scope', 'auth-invalid', 'auth-revoked'])
// 진행 없이 토큰 갱신이 이만큼 연속으로 실패하면 멈춘다. 같은 만료 토큰으로 계속 재접속하지 않는다.
const REFRESHES = 3

export async function runClient({ urls, token, store, startAt, signal, ca, maxAttempts = Infinity, onCommit = () => {}, onFrame = () => {}, onAttempt = () => {}, onDisconnect = () => {} }) {
  const { default: WebSocket } = await import('../edge/node_modules/ws/wrapper.mjs')
  let failures = 0, noProgress = 0, attempt = 0, refreshes = 0, expired
  const pause = delay => new Promise(resolve => {
    const done = () => { clearTimeout(timeout); signal?.removeEventListener('abort', done); resolve() }
    const timeout = setTimeout(done, delay)
    signal?.addEventListener('abort', done, { once: true })
  })
  // 공급자가 signal을 무시해도 취소가 먼저면 끝낸다. 취소 뒤에 늦게 온 결과는 한 번 정해진 Promise가 버린다.
  const acquire = () => new Promise((resolve, reject) => {
    signal?.addEventListener('abort', resolve, { once: true })
    Promise.resolve().then(() => token({ signal })).then(resolve, reject).finally(() => signal?.removeEventListener('abort', resolve))
  })
  while (!signal?.aborted) {
    if (attempt >= maxAttempts) throw new Error('attempt-limit')
    let credential
    try { credential = await acquire() } catch (error) { if (error?.message === 'auth-invalid') throw error }
    if (signal?.aborted) return store.load()
    if (credential === undefined || credential === expired) {
      // 공급자가 토큰을 주지 못했거나 서버가 만료로 거절한 토큰을 다시 주면 접속하지 않고 기다렸다 다시 묻는다.
      if (++refreshes > REFRESHES) throw new Error(credential === undefined ? 'token-unavailable' : 'auth-expired')
      failures = Math.min(failures + 1, 6)
      await pause(Math.random() * Math.min(30000, 500 * 2 ** failures))
      continue
    }
    const before = store.load()
    const application = new Application(store)
    const socket = new WebSocket(urls[attempt++ % urls.length], 'wts.ha.v1', { ca, handshakeTimeout: 3000, maxPayload: 48 * 1024 })
    onAttempt(socket)
    let queued = 0, chain = Promise.resolve(), reason = 'connection-closed', ready = false, applyFailed = false
    let applyError
    const closed = new Promise(resolve => socket.once('close', () => { onDisconnect(reason); resolve() }))
    const stop = () => socket.terminate()
    signal?.addEventListener('abort', stop, { once: true })
    let receiveTimer, receiveCheck, received = performance.now()
    const expire = () => {
      const silent = performance.now() - received
      // 그사이 프레임이 왔거나 타이머가 조금 일찍 깼으면 마지막 수신부터 3초가 될 때까지 다시 기다린다.
      if (silent < 3000) receiveDeadline(3000 - silent)
      else if (queued > 0 && socket.isPaused) receiveDeadline()
      else { reason = 'receive-deadline'; stop() }
    }
    const receiveDeadline = (delay = 3000) => {
      clearTimeout(receiveTimer)
      // 이미 예약한 판단도 취소한다. resume 직후 남은 판단이 새 기한을 건너뛰고 연결을 끊지 않게 한다.
      clearImmediate(receiveCheck)
      // 프레임 처리 중 동기 저장으로 event loop가 막히면 다음 회차의 타이머가 그동안 도착한 프레임보다 먼저 실행된다.
      // 연결이 열린 뒤에는 setImmediate로 한 번 미뤄 쌓인 프레임을 먼저 읽고, 서버의 침묵과 클라이언트의 정지를 가른다.
      receiveTimer = setTimeout(() => { if (socket.readyState === WebSocket.OPEN) receiveCheck = setImmediate(expire); else expire() }, delay)
    }
    receiveDeadline()
    const timer = setInterval(() => {
      if (ready && socket.readyState === WebSocket.OPEN) socket.send('{"type":"heartbeat"}')
    }, 1000)
    socket.on('error', () => { reason = 'connection-error' })
    socket.once('open', () => socket.send(JSON.stringify({ type: 'connect', token: credential })))
    socket.on('message', (data, binary) => {
      received = performance.now()
      receiveDeadline()
      queued += data.length
      if (binary || queued > 1024 * 1024) { reason = 'client-overflow'; return stop() }
      if (queued >= 64 * 1024) socket.pause()
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
      }).catch(error => { applyFailed = true; applyError = error; reason = error.message === 'auth-invalid' ? error.message : 'client-apply-failed'; stop() }).finally(() => {
        queued -= data.length
        if (queued < 32 * 1024 && socket.isPaused && socket.readyState === WebSocket.OPEN) {
          socket.resume(); receiveDeadline()
        }
      })
    })
    await closed
    clearInterval(timer)
    clearTimeout(receiveTimer)
    clearImmediate(receiveCheck)
    signal?.removeEventListener('abort', stop)
    await chain
    if (signal?.aborted) return store.load()
    const after = store.load()
    const progressed = after.offset !== null && (before.offset === null || BigInt(after.offset) > BigInt(before.offset))
    if (terminal.has(reason) || reason === 'client-apply-failed') throw new Error(reason, { cause: applyError })
    if (progressed) refreshes = 0
    if (reason === 'auth-expired') {
      expired = credential
      if (++refreshes > REFRESHES) throw new Error(reason)
    }
    if (!progressed && reason === 'replay-limit') {
      if (++noProgress >= 3) throw new Error('no-checkpoint-progress')
    } else noProgress = 0
    failures = progressed ? 0 : Math.min(failures + 1, 6)
    await pause(attempt === 1 ? Math.random() * 500 : Math.random() * Math.min(30000, 500 * 2 ** failures))
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
  } catch (error) {
    // 토큰, cursor, 파일 경로가 섞일 수 있는 원문 대신 정해 둔 코드만 내보낸다.
    const codes = new Set([...terminal, 'auth-expired', 'token-unavailable', 'client-apply-failed', 'no-checkpoint-progress', 'invalid-options', 'invalid-endpoint'])
    console.error(JSON.stringify({ error: codes.has(error?.message) ? error.message : 'client-failed' }))
    process.exitCode = 1
  }
}

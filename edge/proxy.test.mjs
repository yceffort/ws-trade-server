import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import WebSocket, { WebSocketServer } from 'ws'
import http from 'node:http'
import net from 'node:net'
import { REASONS, startProxy } from './proxy.mjs'
import { certificate, echoServer } from './fixtures.mjs'

test('HA route preserves its path and protocol and rejects mixed negotiations', { timeout: 10000 }, async (t) => {
  const backend = new WebSocketServer({ host: '127.0.0.1', port: 0, handleProtocols: (protocols) => protocols.has('wts.ha.v1') && 'wts.ha.v1' })
  await once(backend, 'listening')
  t.after(async () => { for (const socket of backend.clients) socket.terminate(); await new Promise(resolve => backend.close(resolve)) })
  const proxy = await startProxy({ backend: `ws://127.0.0.1:${backend.address().port}/ws` })
  t.after(() => proxy.close())
  const connection = once(backend, 'connection')
  const socket = new WebSocket(`ws://127.0.0.1:${proxy.address.port}/experimental/ha/ws`, 'wts.ha.v1')
  t.after(() => socket.terminate())
  await once(socket, 'open')
  const [upstream, request] = await connection
  assert.equal(request.url, '/experimental/ha/ws')
  assert.equal(socket.protocol, 'wts.ha.v1')
  assert.equal(upstream.protocol, 'wts.ha.v1')
  for (const [path, protocol] of [['/ws', 'wts.ha.v1'], ['/experimental/ha/ws', 'v12.stomp'], ['/experimental/ha/ws?token=x', 'wts.ha.v1']]) {
    const rejected = new WebSocket(`ws://127.0.0.1:${proxy.address.port}${path}`, protocol)
    await assert.rejects(once(rejected, 'open'), /400|404/)
  }
})

test('upstream drain preserves only 503 and a bounded Retry-After', { timeout: 10000 }, async (t) => {
  const backend = http.createServer()
  backend.on('upgrade', (_, socket) => socket.end('HTTP/1.1 503 Service Unavailable\r\nRetry-After: 7\r\nX-Private: hidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'))
  backend.listen(0, '127.0.0.1'); await once(backend, 'listening')
  t.after(() => new Promise(resolve => backend.close(resolve)))
  const proxy = await startProxy({ backend: `ws://127.0.0.1:${backend.address().port}/ws` })
  t.after(() => proxy.close())
  const socket = new WebSocket(`ws://127.0.0.1:${proxy.address.port}/experimental/ha/ws`, 'wts.ha.v1')
  socket.on('error', () => {})
  t.after(() => socket.terminate())
  const [, response] = await once(socket, 'unexpected-response')
  assert.equal(response.statusCode, 503)
  assert.equal(response.headers['retry-after'], '7')
  assert.equal(response.headers['x-private'], undefined)
  response.destroy()
  assert.equal(proxy.counts().upstream_unavailable, 1)
})

test('rejections and closes are counted once by fixed reasons only', { timeout: 10000 }, async (t) => {
  const backend = await echoServer(); t.after(() => backend.close())
  const proxy = await startProxy({ backend: backend.url }); t.after(() => proxy.close())
  const base = `ws://127.0.0.1:${proxy.address.port}`
  await assert.rejects(once(new WebSocket(`${base}/unknown`, 'v12.stomp'), 'open'), /404/)
  await assert.rejects(once(new WebSocket(`${base}/ws`, 'wts.ha.v1'), 'open'), /400/)
  const socket = new WebSocket(`${base}/ws`, 'v12.stomp')
  await once(socket, 'open')
  socket.close()
  await once(socket, 'close')
  while (proxy.counts().client_closed === 0) await new Promise((resolve) => setTimeout(resolve, 10))
  assert.deepEqual(Object.keys(proxy.counts()), REASONS)
  assert.deepEqual(Object.fromEntries(Object.entries(proxy.counts()).filter(([, n]) => n > 0)), { not_found: 1, bad_protocol: 1, client_closed: 1 })
})

test('TLS verifies trust and preserves protocol, message boundaries and close', { timeout: 10000 }, async (t) => {
  const cert = certificate(); t.after(cert.close)
  const backend = await echoServer(); t.after(() => backend.close())
  const proxy = await startProxy({ backend: backend.url, tls: cert.tls }); t.after(() => proxy.close())
  const url = `wss://127.0.0.1:${proxy.address.port}/ws`
  const untrusted = new WebSocket(url, 'v12.stomp')
  await assert.rejects(once(untrusted, 'open'), /self.signed|certificate/)
  // TLS가 아닌 요청은 핸드셰이크 실패로, 인증서를 믿지 않아 끊은 클라이언트는 중단으로 센다.
  const plain = net.connect(proxy.address.port, '127.0.0.1', () => plain.end('GET /ws HTTP/1.1\r\n\r\n'))
  await once(plain, 'close')
  while (proxy.counts().tls_handshake_failed + proxy.counts().tls_aborted < 2) await new Promise((resolve) => setTimeout(resolve, 10))
  assert.deepEqual([proxy.counts().tls_aborted, proxy.counts().tls_handshake_failed], [1, 1])
  const socket = new WebSocket(url, 'v12.stomp', { ca: cert.tls.cert })
  await once(socket, 'open'); t.after(() => socket.terminate())
  assert.equal(socket.protocol, 'v12.stomp')
  for (const payload of ['CONNECT\nconnection-id:demo\n\n\0', '\n', 'MESSAGE\nsubscription:1\n\n{"price":123}\0']) {
    const received = once(socket, 'message'); socket.send(payload)
    const [data, binary] = await received
    assert.equal(data.toString(), payload); assert.equal(binary, false)
  }
  const pong = once(socket, 'pong'); socket.ping('activity'); assert.equal((await pong)[0].toString(), 'activity')
  const closed = once(socket, 'close')
  for (const upstream of backend.server.clients) upstream.close(1001, 'shutdown')
  assert.equal((await closed)[0], 1001)
  for (const [path, protocol] of [['/wrong', 'v12.stomp'], ['/ws', 'other']]) {
    const bad = new WebSocket(`wss://127.0.0.1:${proxy.address.port}${path}`, protocol, { ca: cert.tls.cert })
    await assert.rejects(once(bad, 'open'), /404|400/)
  }
})

test('client close code and reason reach the backend', { timeout: 10000 }, async (t) => {
  const backend = await echoServer(); t.after(() => backend.close())
  const proxy = await startProxy({ backend: backend.url }); t.after(() => proxy.close())
  const connected = once(backend.server, 'connection')
  const socket = new WebSocket(`ws://127.0.0.1:${proxy.address.port}/ws`, 'v12.stomp')
  t.after(() => socket.terminate()); await once(socket, 'open')
  const [upstream] = await connected, closed = once(upstream, 'close')
  socket.close(1000, 'done')
  const [code, reason] = await closed
  assert.equal(code, 1000); assert.equal(reason.toString(), 'done')
})

test('control frames use the same bounded send budget', { timeout: 10000 }, async (t) => {
  const backend = await echoServer(); t.after(() => backend.close())
  const proxy = await startProxy({ backend: backend.url, maxBuffer: 16 }); t.after(() => proxy.close())
  for (const control of ['ping', 'pong']) {
    const socket = new WebSocket(`ws://127.0.0.1:${proxy.address.port}/ws`, 'v12.stomp')
    t.after(() => socket.terminate()); await once(socket, 'open')
    const closed = once(socket, 'close')
    socket[control](Buffer.alloc(32))
    await closed
  }
})

test('a paused TLS client drains a replay burst without overflowing the proxy', { timeout: 10000 }, async (t) => {
  const cert = certificate(); t.after(cert.close)
  const backend = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await once(backend, 'listening')
  t.after(async () => { for (const socket of backend.clients) socket.terminate(); await new Promise(resolve => backend.close(resolve)) })
  backend.on('connection', socket => socket.once('message', () => {
    const payload = Buffer.alloc(32 * 1024, 7)
    for (let i = 0; i < 512; i++) socket.send(payload)
  }))
  const proxy = await startProxy({ backend: `ws://127.0.0.1:${backend.address().port}/ws`, tls: cert.tls, maxBuffer: 128 * 1024 })
  t.after(() => proxy.close())
  const socket = new WebSocket(`wss://127.0.0.1:${proxy.address.port}/ws`, 'v12.stomp', { ca: cert.tls.cert })
  t.after(() => socket.terminate()); await once(socket, 'open')
  let count = 0
  const received = new Promise((resolve, reject) => {
    socket.on('message', data => {
      assert.equal(data.length, 32 * 1024)
      assert.equal(data[0], 7)
      if (++count === 512) resolve()
    })
    socket.once('close', () => { if (count !== 512) reject(new Error(`replay closed at ${count}`)) })
    socket.once('error', reject)
  })
  socket.pause(); socket.send('replay')
  const timer = setTimeout(() => socket.resume(), 300)
  t.after(() => clearTimeout(timer))
  await received
  assert.equal(socket.readyState, WebSocket.OPEN)
})

test('abandoning a paused replay releases the backend close handshake', { timeout: 10000 }, async (t) => {
  const backend = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await once(backend, 'listening')
  t.after(async () => { for (const socket of backend.clients) socket.terminate(); await new Promise(resolve => backend.close(resolve)) })
  const proxy = await startProxy({ backend: `ws://127.0.0.1:${backend.address().port}/ws`, maxBuffer: 128 * 1024 })
  t.after(() => proxy.close())
  const connected = once(backend, 'connection')
  const socket = new WebSocket(`ws://127.0.0.1:${proxy.address.port}/ws`, 'v12.stomp')
  t.after(() => socket.terminate()); await once(socket, 'open')
  const [upstream] = await connected
  socket.pause()
  const payload = Buffer.alloc(32 * 1024)
  for (let i = 0; i < 512; i++) upstream.send(payload)
  await new Promise(resolve => setTimeout(resolve, 300))
  const closed = once(upstream, 'close', { signal: AbortSignal.timeout(2000) })
  socket.terminate()
  await closed
})

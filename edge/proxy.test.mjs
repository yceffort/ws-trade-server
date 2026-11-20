import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import WebSocket from 'ws'
import { startProxy } from './proxy.mjs'
import { certificate, echoServer } from './fixtures.mjs'

test('TLS verifies trust and preserves protocol, message boundaries and close', { timeout: 10000 }, async (t) => {
  const cert = certificate(); t.after(cert.close)
  const backend = await echoServer(); t.after(() => backend.close())
  const proxy = await startProxy({ backend: backend.url, tls: cert.tls }); t.after(() => proxy.close())
  const url = `wss://127.0.0.1:${proxy.address.port}/ws`
  const untrusted = new WebSocket(url, 'v12.stomp')
  await assert.rejects(once(untrusted, 'open'), /self.signed|certificate/)
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

import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import WebSocket from 'ws'
import { startProxy } from './proxy.mjs'
import { certificate, echoServer } from './fixtures.mjs'

test('deflate is negotiated only when both peers enable it and payloads stay identical', { timeout: 10000 }, async (t) => {
  const backend = await echoServer(); t.after(() => backend.close())
  const cert = certificate(); t.after(cert.close)
  for (const enabled of [false, true]) {
    const proxy = await startProxy({ backend: backend.url, tls: cert.tls, compression: enabled }); t.after(() => proxy.close())
    for (const offered of [false, true]) {
      const socket = new WebSocket(`wss://127.0.0.1:${proxy.address.port}/ws`, 'v12.stomp', { ca: cert.tls.cert, perMessageDeflate: offered })
      t.after(() => socket.terminate()); await once(socket, 'open')
      assert.equal(socket.extensions.includes('permessage-deflate'), enabled && offered)
      const payload = 'MESSAGE\nsubscription:demo\n\n' + JSON.stringify({ price: 12345, amount: 'synthetic'.repeat(100) }) + '\0'
      const received = once(socket, 'message'); socket.send(payload)
      assert.equal((await received)[0].toString(), payload)
      socket.close(); await once(socket, 'close')
    }
  }
})

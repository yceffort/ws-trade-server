import assert from 'node:assert/strict'
import { once } from 'node:events'
import test from 'node:test'
import { WebSocketServer } from '../edge/node_modules/ws/wrapper.mjs'
import { runClient } from './ha-client.mjs'
import { MemoryStore } from './ha-state.mjs'

async function server() {
  const server = new WebSocketServer({host:'127.0.0.1',port:0,handleProtocols:()=>'wts.ha.v1'})
  await once(server,'listening')
  return server
}
const options = server => ({ urls:[`ws://127.0.0.1:${server.address().port}/experimental/ha/ws`],token:()=> 'Bearer test',store:new MemoryStore('events',['a']),startAt:{log_id:'test',partition:0,offset:'0'} })
function close(server) { for (const client of server.clients) client.terminate(); server.close() }

test('three unchanged attempts without a first checkpoint stop',async () => {
  const mock = await server()
  let attempts = 0
  mock.on('connection', socket => {
    attempts++
    socket.on('message',bytes => {
      const frame = JSON.parse(bytes)
      if (frame.type === 'connect') socket.send('{"type":"connected"}')
      if (frame.type === 'resume') socket.send('{"type":"error","code":"replay-limit"}')
    })
  })
  const random = Math.random
  Math.random = () => 0
  try { await assert.rejects(runClient(options(mock)),/no-checkpoint-progress/); assert.equal(attempts,3) }
  finally { Math.random = random; close(mock) }
})

test('a silent server is detected at the receive deadline despite outgoing heartbeats',async () => {
  const mock = await server(), controller = new AbortController()
  let first, elapsed, heartbeats = 0
  mock.on('connection',socket => {
    if (first !== undefined) { elapsed = performance.now() - first; controller.abort(); return }
    first = performance.now()
    socket.on('message',bytes => {
      const frame = JSON.parse(bytes)
      if (frame.type === 'connect') socket.send('{"type":"connected"}')
      if (frame.type === 'heartbeat') heartbeats++
    })
  })
  const random = Math.random
  Math.random = () => 0
  try {
    await runClient({...options(mock),signal:controller.signal})
    assert.ok(elapsed >= 2900 && elapsed < 3700,`deadline elapsed ${elapsed}`)
    assert.ok(heartbeats >= 2)
  } finally { Math.random = random; close(mock) }
})

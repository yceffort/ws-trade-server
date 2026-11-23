import test from 'node:test'
import assert from 'node:assert/strict'
import net from 'node:net'
import { createServer, request } from 'node:http'
import { once } from 'node:events'
import { spawn } from 'node:child_process'
import { mkdtempSync, writeFileSync, existsSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import WebSocket, { WebSocketServer } from '../../edge/node_modules/ws/wrapper.mjs'
import { runClient } from '../../examples/ha-client.js'
import { watch } from './reasons.js'

const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
// events.once는 error가 먼저 나오면 거절하므로 close만 직접 기다린다.
const closed = socket => new Promise(resolve => socket.once('close', resolve))
const silent = () => net.createServer(() => {})
const rejecting = status => () =>
  createServer().on('upgrade', (_, socket) =>
    socket.end(`HTTP/1.1 ${status} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`)
  )
const accepting = onConnection => () => {
  const server = createServer()
  new WebSocketServer({ server, handleProtocols: () => 'wts.ha.v1' }).on('connection', onConnection)
  return server
}

// 실제 소켓으로 각 실패를 만들고, 닫힌 뒤 기억된 사유를 돌려준다.
async function failure(t, server, { options = {}, act = () => {} } = {}) {
  const sockets = new Set()
  server.on('connection', socket => sockets.add(socket))
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  t.after(() => {
    for (const socket of sockets) socket.destroy()
    server.close()
  })
  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}/experimental/ha/ws`, 'wts.ha.v1', options)
  const failed = watch(client)
  act(client)
  await closed(client)
  return failed()
}

const cases = {
  'handshake-timeout': [silent, { options: { handshakeTimeout: 100 } }],
  'connect-aborted': [silent, { act: client => client.terminate() }],
  'rejected-502': [rejecting(502)],
  'rejected-503': [rejecting(503)],
  'rejected-other': [rejecting(404)],
  'connect-reset': [() => net.createServer(socket => socket.resetAndDestroy())],
  'connect-error': [() => net.createServer(socket => socket.end('not http\r\n\r\n'))],
  'connection-error': [accepting(socket => socket.send('x'.repeat(64))), { options: { maxPayload: 16 } }]
}
for (const [reason, [server, how]] of Object.entries(cases)) {
  test(`접속 실패를 ${reason}로 분류한다`, { timeout: 5000 }, async t => {
    assert.equal(await failure(t, server(), how), reason)
  })
}

test('닫힌 포트로의 접속을 connect-refused로 분류한다', { timeout: 5000 }, async () => {
  const server = net.createServer().listen(0, '127.0.0.1')
  await once(server, 'listening')
  const { port } = server.address()
  await new Promise(resolve => server.close(resolve))
  const client = new WebSocket(`ws://127.0.0.1:${port}/experimental/ha/ws`, 'wts.ha.v1')
  const failed = watch(client)
  await closed(client)
  assert.equal(failed(), 'connect-refused')
})

test('오류 없이 닫힌 연결에는 사유를 붙이지 않는다', { timeout: 5000 }, async t => {
  assert.equal(await failure(t, accepting(socket => socket.close())()), null)
})

test(
  '예제 클라이언트의 수신 기한 3초가 handshake 중인 소켓을 끊으면 connect-aborted로 센다',
  { timeout: 10000 },
  async t => {
    const server = silent().listen(0, '127.0.0.1')
    const sockets = new Set()
    server.on('connection', socket => sockets.add(socket))
    await once(server, 'listening')
    t.after(() => {
      for (const socket of sockets) socket.destroy()
      server.close()
    })
    let failed, seen
    const store = { load: () => ({ topics: ['a'], mode: 'state', offset: null }) }
    await assert.rejects(
      runClient({
        urls: [`ws://127.0.0.1:${server.address().port}/experimental/ha/ws`],
        token: () => 'Bearer synthetic',
        store,
        maxAttempts: 1,
        onAttempt: socket => {
          failed = watch(socket)
        },
        onDisconnect: reason => {
          seen = [reason, failed()]
        }
      }),
      /attempt-limit/
    )
    assert.deepEqual(seen, ['connection-error', 'connect-aborted'])
  }
)

test('부하 실행기는 접속 실패를 사유별 누적 횟수로 보고한다', { timeout: 20000 }, async () => {
  const directory = mkdtempSync(join(tmpdir(), 'wts-reasons-'))
  const sockets = new WebSocketServer({ noServer: true, handleProtocols: () => 'wts.ha.v1' })
  const timers = new Set()
  sockets.on('connection', socket => {
    const timer = setInterval(() => {
      if (socket.readyState === 1) socket.send('{"type":"heartbeat"}')
    }, 200)
    timers.add(timer)
    socket.on('close', () => {
      clearInterval(timer)
      timers.delete(timer)
    })
    socket.on('message', bytes => {
      const frame = JSON.parse(bytes)
      if (frame.type === 'connect') socket.send('{"type":"connected"}')
      if (frame.type === 'resume') {
        socket.send(
          JSON.stringify({
            type: 'event',
            event_id: 'demo/trades/epoch/1',
            topic: 'a',
            position: { log_id: 'test', partition: 0, offset: '1' },
            body: { price: '1' }
          })
        )
        socket.send('{"type":"checkpoint","offset":"1","cursor":"synthetic-cursor"}')
        socket.send('{"type":"caught-up","offset":"1"}')
      }
    })
  })
  let upgrades = 0
  const server = createServer()
    .on('upgrade', (incoming, socket, head) => {
      if (++upgrades <= 2)
        socket.end(
          'HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'
        )
      else sockets.handleUpgrade(incoming, socket, head, client => sockets.emit('connection', client))
    })
    .listen(0, '127.0.0.1')
  await once(server, 'listening')
  let child
  try {
    const oracle = join(directory, 'oracle.js'),
      control = join(directory, 'control.sock')
    writeFileSync(oracle, `console.log(JSON.stringify({log_id:'test',next:'1',topics:['a']}))`)
    writeFileSync(join(directory, 'token'), 'Bearer synthetic')
    writeFileSync(
      join(directory, 'config.json'),
      JSON.stringify({
        clients: 1,
        workers: 1,
        durationSeconds: 10,
        requireReconnectSlo: false,
        urls: [`ws://127.0.0.1:${server.address().port}/experimental/ha/ws`],
        stateDirectory: join(directory, 'state'),
        controlSocket: control,
        output: join(directory, 'report.json'),
        tokenFile: join(directory, 'token'),
        headCommand: [process.execPath, oracle],
        rangeCommand: [process.execPath, oracle]
      })
    )
    child = spawn(
      process.execPath,
      [fileURLToPath(new URL('./run.js', import.meta.url)), join(directory, 'config.json')],
      { stdio: 'ignore' }
    )
    for (let i = 0; i < 500 && !existsSync(control); i++) await delay(20)
    assert.ok(existsSync(control))
    const status = await new Promise((resolve, reject) => {
      request({ socketPath: control, path: '/status', timeout: 5000 }, response => {
        let text = ''
        response.on('data', chunk => {
          text += chunk
        })
        response.on('end', () => resolve(JSON.parse(text)))
      })
        .on('error', reject)
        .end()
    })
    assert.deepEqual(status.disconnect_reasons, { 'rejected-503': 2 })
    assert.deepEqual(status.reasons, { 'rejected-503': 1 })
  } finally {
    if (child?.exitCode === null) {
      child.kill('SIGKILL')
      await once(child, 'exit')
    }
    for (const timer of timers) clearInterval(timer)
    for (const socket of sockets.clients) socket.terminate()
    await new Promise(resolve => server.close(resolve))
    rmSync(directory, { recursive: true, force: true })
  }
})

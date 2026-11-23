import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn, execFile } from 'node:child_process'
import { once } from 'node:events'
import { createInterface } from 'node:readline'
import { promisify } from 'node:util'
import { fileURLToPath } from 'node:url'

for (const host of ['', '0.0.0.0', '::1', '::']) {
  test(`CLI binding and probe agree for host ${JSON.stringify(host)}`, { timeout: 10000 }, async (t) => {
    const env = { ...process.env, WTS_EDGE_HOST: host, WTS_EDGE_PORT: '0', WTS_TLS_CERT_FILE: '', WTS_TLS_KEY_FILE: '' }
    const child = spawn(process.execPath, [fileURLToPath(new URL('run.js', import.meta.url))], { env, stdio: ['ignore', 'ignore', 'pipe'] })
    const exited = once(child, 'exit')
    const lines = createInterface({ input: child.stderr })
    t.after(async () => { child.kill('SIGTERM'); await exited; lines.close() })
    const [line] = await once(lines, 'line')
    const { level, target, fields } = JSON.parse(line)
    assert.deepEqual([level, target, fields.event], ['INFO', 'edge', 'edge_started'])
    assert(fields.url.startsWith(`ws://${host || '127.0.0.1'}:`), line)
    const port = fields.url.match(/:(\d+)\/ws$/)?.[1]
    assert(port, line)
    await promisify(execFile)(process.execPath, [fileURLToPath(new URL('tcp-probe.js', import.meta.url))], {
      env: { ...env, WTS_EDGE_PORT: port }, timeout: 3000,
    })
    child.kill('SIGTERM')
    assert.equal((await exited)[0], 0)
  })
}

const edge = (env) => {
  const child = spawn(process.execPath, [fileURLToPath(new URL('run.js', import.meta.url))], { env: { ...process.env, WTS_EDGE_PORT: '0', WTS_TLS_CERT_FILE: '', WTS_TLS_KEY_FILE: '', ...env }, stdio: ['ignore', 'ignore', 'pipe'] })
  const events = []
  // worker가 시작하지 못하면 stack trace처럼 JSON이 아닌 줄도 stderr에 나온다.
  createInterface({ input: child.stderr }).on('line', (line) => { try { events.push(JSON.parse(line).fields) } catch {} })
  const until = async (predicate) => { while (!predicate(events)) await new Promise((resolve) => setTimeout(resolve, 20)) }
  // exit 뒤에도 stderr가 남아 있을 수 있어 close까지 기다린 뒤 로그를 검사한다.
  return { child, events, until, exited: once(child, 'close') }
}

test('workers share one port, relay connections and stop together', { timeout: 15000 }, async (t) => {
  const { echoServer } = await import('./fixtures.js')
  const { default: WebSocket } = await import('ws')
  const backend = await echoServer()
  t.after(() => backend.close())
  const { child, events, until, exited } = edge({ WTS_BACKEND: backend.url, WTS_EDGE_WORKERS: '2' })
  await until((list) => list.filter((e) => e.event === 'edge_started').length === 2)
  const started = events.filter((e) => e.event === 'edge_started')
  assert.notEqual(started[0].worker, started[1].worker)
  assert.equal(new Set(started.map((e) => e.url)).size, 1, 'workers must listen on the same port')
  const replies = await Promise.all(Array.from({ length: 8 }, async (_, i) => {
    const socket = new WebSocket(started[0].url, 'v12.stomp')
    await once(socket, 'open')
    socket.send(`ping-${i}`)
    const [data] = await once(socket, 'message')
    socket.close()
    return data.toString()
  }))
  assert.deepEqual(replies, Array.from({ length: 8 }, (_, i) => `ping-${i}`))
  child.kill('SIGTERM')
  assert.equal((await exited)[0], 0)
  assert.equal(events.filter((e) => e.event === 'edge_stopped' && e.worker).length, 2)
  assert.ok(events.at(-1).event === 'edge_stopped' && !events.at(-1).worker, 'the primary reports the stop last')
})

test('a crashed worker is replaced without stopping the edge', { timeout: 15000 }, async (t) => {
  const { child, events, until, exited } = edge({ WTS_EDGE_WORKERS: '2' })
  t.after(async () => { child.kill('SIGTERM'); await exited })
  await until((list) => list.filter((e) => e.event === 'edge_started').length === 2)
  const pids = (await promisify(execFile)('pgrep', ['-P', String(child.pid)])).stdout.trim().split('\n').map(Number)
  process.kill(pids[0], 'SIGKILL')
  await until((list) => list.some((e) => e.event === 'edge_worker_exited') && list.filter((e) => e.event === 'edge_started').length === 3)
  assert.equal(child.exitCode, null)
})

test('workers that cannot listen fail the edge instead of restarting forever', { timeout: 15000 }, async (t) => {
  const { createServer } = await import('node:net')
  const taken = createServer().listen(0, '127.0.0.1')
  await once(taken, 'listening')
  t.after(() => taken.close())
  const { child, events, exited } = edge({ WTS_EDGE_WORKERS: '2', WTS_EDGE_PORT: String(taken.address().port) })
  assert.equal((await exited)[0], 1)
  assert.ok(!events.some((e) => e.event === 'edge_worker_exited'))
  child.kill('SIGTERM')
})

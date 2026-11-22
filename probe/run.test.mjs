import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { WebSocketServer } from '../edge/node_modules/ws/wrapper.mjs'
import { probe, validate } from './run.mjs'
import { Metrics } from './server.mjs'

const config = { target: 'ha-local', protocol: 'ha', url: 'ws://127.0.0.1:1/experimental/ha/ws', topics: ['a'], deadlineMs: 3000 }
async function mock(t, respond) {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0, handleProtocols: () => 'wts.ha.v1' })
  await once(server, 'listening')
  let count = 0
  server.on('connection', socket => { const attempt = ++count; socket.on('message', bytes => respond(JSON.parse(bytes), socket, attempt)) })
  t.after(() => { for (const client of server.clients) client.terminate(); server.close() })
  return { ...config, url: `ws://127.0.0.1:${server.address().port}/experimental/ha/ws` }
}
const events = [1, 2].map(n => ({ offset: String(n), id: `demo/trades/epoch/${n}`, topic: 'a', body: { price: n } }))
const dependencies = { token: () => 'Bearer synthetic', command: async (argv, extra) => extra ? { log_id: 'test', start: '1', through: extra[1], events } : { log_id: 'test', next: '1', topics: ['a'] } }
function send(socket, n) {
  const e = events[n - 1]
  socket.send(JSON.stringify({ type: 'event', event_id: e.id, topic: e.topic, body: e.body, position: { log_id: 'test', partition: 0, offset: e.offset } }))
  socket.send(JSON.stringify({ type: 'checkpoint', offset: e.offset, cursor: `private-cursor-${n}` }))
  socket.send('{"type":"caught-up"}')
}
test('HA는 재접속 뒤 checkpoint 적용과 권한 범위 oracle를 대조한다', async t => {
  const conf = await mock(t, (frame, socket, attempt) => { if (frame.type === 'connect') socket.send('{"type":"connected"}'); if (frame.type === 'resume') send(socket, attempt) })
  const result = await probe(conf, dependencies)
  assert.equal(result.ok, true); assert.equal(result.attempts, 2)
  assert.ok(result.samples.oracle >= 0)
  assert.doesNotMatch(JSON.stringify(result), /private-cursor|synthetic|price|demo\/trades/)
})
test('인증 거절은 auth 단계로 제한한다', async t => {
  const conf = await mock(t, (frame, socket) => { if (frame.type === 'connect') socket.send('{"type":"error","code":"auth-invalid"}') })
  const result = await probe(conf, dependencies)
  assert.equal(result.ok, false); assert.equal(result.stage, 'auth')
})
test('heartbeat만 있고 체결이 멈춘 경로는 성공하지 않는다', async t => {
  const conf = await mock(t, (frame, socket) => { if (frame.type === 'connect') socket.send('{"type":"connected"}'); if (frame.type === 'resume') socket.send('{"type":"caught-up"}'); if (frame.type === 'heartbeat') socket.send('{"type":"heartbeat"}') })
  const result = await probe({ ...conf, deadlineMs: 150 }, dependencies)
  assert.equal(result.ok, false); assert.equal(result.stage, 'first_event')
})
test('전달된 본문이 committed oracle와 다르면 실패한다', async t => {
  const conf = await mock(t, (frame, socket, attempt) => { if (frame.type === 'connect') socket.send('{"type":"connected"}'); if (frame.type === 'resume') send(socket, attempt) })
  const result = await probe(conf, { ...dependencies, command: async (argv, extra) => extra ? { log_id: 'test', start: '1', through: '2', events: events.map(e => ({ ...e, body: {} })) } : dependencies.command(argv) })
  assert.equal(result.ok, false); assert.equal(result.stage, 'oracle')
})
test('처음에는 성공과 지연 표본을 만들지 않고 실패에 비밀 값을 남기지 않는다', () => {
  const metrics = new Metrics([config])
  assert.doesNotMatch(metrics.text(), /wts_probe_success|wts_probe_stage_seconds/)
  metrics.record(metrics.targets[0], { ok: false, stage: 'tls', samples: { auth: 1 } })
  assert.match(metrics.text(), /wts_probe_success.* 0/)
  assert.doesNotMatch(metrics.text(), /wts_probe_stage_seconds/)
  metrics.record(metrics.targets[0], { ok: true, stage: 'apply', samples: { apply: 0.1 } })
  assert.match(metrics.text(), /wts_probe_stage_seconds_count.* 1/)
})
test('원격 평문 주소와 높은 cardinality 입력을 거절한다', () => {
  for (const overrides of [{ url: 'ws://example.org/experimental/ha/ws' }, { target: 'token=secret' }, { deadlineMs: 100000 }, { topics: Array(5).fill('a') }]) assert.throws(() => validate({ ...config, ...overrides }))
})

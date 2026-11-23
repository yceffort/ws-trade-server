import test from 'node:test'
import assert from 'node:assert/strict'
import { Environment } from '../bench/soak/environment.js'
import { Client, until } from '../bench/soak/client.js'
import { checkWebSocket } from '../examples/websocket-check.js'

test('실제 feed와 gateway의 WS 및 WSS 클라이언트 경로', { timeout: 45000 }, async t => {
  const controller = new AbortController()
  const abort = () => controller.abort()
  for (const name of ['SIGINT', 'SIGTERM']) process.on(name, abort)
  const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(35000)])
  const env = new Environment({ binDir: 'target/debug', connections: 4 }, signal)
  try {
    await env.start()
    for (const [name, url] of [
      ['WS', env.direct],
      ['WSS', env.url]
    ]) {
      await t.test(`${name} 인증, 구독, 수신, 해지와 재접속`, async () => {
        const result = await checkWebSocket({ url, token: env.stable, ca: env.ca, signal })
        assert.equal(result.ok, true)
        assert.equal(result.sessions.length, 2)
        assert.deepEqual(
          result.steps.map(step => step.name),
          [
            'connect-subscribe',
            'receive',
            'unsubscribe',
            'disconnect',
            'reconnect-subscribe',
            'receive-after-reconnect',
            'close'
          ]
        )
      })
    }
    await t.test('잘못된 토큰은 인증 단계에서 거절', async () => {
      const client = new Client(env.url, 'invalid-synthetic-token', env.ca, signal)
      try {
        await until(() => client.closed, 'auth rejection', signal)
        client.valid()
        assert.equal(client.connected, false)
        assert.equal(client.messages, 0)
        assert.equal(client.serverError, 'auth-invalid')
      } finally {
        client.terminate()
      }
    })
    await t.test('신뢰하지 않은 인증서는 WSS 연결 실패', async () => {
      await assert.rejects(checkWebSocket({ url: env.url, token: env.stable, signal }), {
        message: 'websocket-check-failed:connect-subscribe'
      })
    })
    await until(
      async () => {
        const metrics = await env.json('metrics')
        return metrics.stomp_connections === 0 && metrics.subscriptions === 0 && metrics.queued_frames === 0
      },
      'connection cleanup',
      signal
    )
    assert.equal(env.invalidLogs, 0)
  } finally {
    try {
      await env.close()
    } finally {
      for (const name of ['SIGINT', 'SIGTERM']) process.off(name, abort)
    }
  }
  assert.equal(env.gateway?.exitCode, 0)
  assert.equal(env.events.shutdown_complete, 1)
})

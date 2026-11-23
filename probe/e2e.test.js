import test from 'node:test'
import assert from 'node:assert/strict'
import { Environment } from '../bench/soak/environment.js'
import { probe } from './run.js'

test('실제 WSS의 정상, TLS 실패, 인증 실패, 데이터 정지를 구분한다', { timeout: 30000 }, async t => {
  const env = new Environment(
    { binDir: process.env.WTS_BIN_DIR ?? 'target/debug', connections: 4 },
    AbortSignal.timeout(25000)
  )
  try {
    await env.start()
    const config = { target: 'local-wss', protocol: 'stomp', url: env.url, deadlineMs: 3000 }
    const dependencies = { token: () => env.stable, ca: env.ca }
    await t.test('인증서 검증과 재접속 뒤 상태 적용', async () => {
      const result = await probe(config, dependencies)
      assert.equal(result.ok, true)
      assert.equal(result.attempts, 2)
      assert.ok(result.samples.tls >= 0)
      assert.ok(result.samples.apply >= 0)
    })
    await t.test('신뢰하지 않는 인증서는 TLS 실패', async () => {
      const result = await probe(config, { token: () => env.stable })
      assert.equal(result.ok, false)
      assert.equal(result.stage, 'tls')
    })
    await t.test('인증 거절', async () => {
      const result = await probe(config, { ...dependencies, token: () => 'synthetic-invalid' })
      assert.equal(result.ok, false)
      assert.equal(result.stage, 'auth')
    })
    await t.test('데이터 정지', async () => {
      await env.stop(env.feed)
      const result = await probe({ ...config, deadlineMs: 300 }, dependencies)
      assert.equal(result.ok, false)
      assert.equal(result.stage, 'first_event')
    })
  } finally {
    await env.close()
  }
})

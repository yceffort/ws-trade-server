import test from 'node:test'
import assert from 'node:assert/strict'
import { checkWebSocket, endpointUrl } from './websocket-check.js'

test('원격 서버는 WSS만 허용하고 loopback WS는 허용한다', () => {
  for (const url of ['wss://example.com/ws', 'ws://localhost:8080/ws', 'ws://127.0.0.1:8080/ws', 'ws://[::1]:8080/ws']) {
    assert.equal(endpointUrl(url), url)
  }
  for (const url of ['ws://192.0.2.10:8080/ws', 'ws://example.com/ws', 'ws://localhost.example.com/ws',
    'https://example.com/ws', 'wss://example.com/other', 'wss://user:password@example.com/ws',
    'wss://example.com/ws?token=synthetic', 'wss://example.com/ws#fragment']) {
    assert.throws(() => endpointUrl(url))
  }
})

test('직접 호출한 클라이언트도 소켓 생성 전에 원격 WS를 거절한다', async () => {
  await assert.rejects(checkWebSocket({ url: 'ws://192.0.2.10:8080/ws', token: 'synthetic-only' }),
    { code: 'ERR_ASSERTION' })
})

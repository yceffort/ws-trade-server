// 합성 캡처로 추출기를 검사한다. 사용: node --test capture/
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'

const EXTRACTOR = join(import.meta.dirname, 'extract-contract.js')
const TOPIC = '/topic/v1/kr/stock/trade/TEST'

const frame = (command, headers, body = '') =>
  `${command}\n${Object.entries(headers)
    .map(([key, value]) => `${key}:${value}\n`)
    .join('')}\n${body}\0`

// steps는 ['sent' | 'received', 프레임] 목록이다. 적힌 순서가 캡처에 기록된 순서다.
function extract(steps) {
  const handshake = [
    { event: 'webSocketCreated', url: 'ws://localhost/ws' },
    { event: 'webSocketWillSendHandshakeRequest', request: { headers: { 'Sec-WebSocket-Protocol': 'v12.stomp' } } },
    { event: 'webSocketHandshakeResponseReceived', response: { headers: { 'sec-websocket-protocol': 'v12.stomp' } } }
  ]
  const frames = steps.map(([direction, payloadData]) => ({
    event: direction === 'sent' ? 'webSocketFrameSent' : 'webSocketFrameReceived',
    response: { opcode: 1, payloadData }
  }))
  const dir = mkdtempSync(join(tmpdir(), 'extract-contract-'))
  try {
    const file = join(dir, 'frames.jsonl')
    writeFileSync(file, [...handshake, ...frames].map((record, ms) => JSON.stringify({ ms, ...record })).join('\n'))
    return JSON.parse(execFileSync(process.execPath, [EXTRACTOR, file], { encoding: 'utf8' }))
  } finally {
    rmSync(dir, { recursive: true })
  }
}

const connect = [
  ['sent', frame('CONNECT', { 'connection-id': 'c' })],
  ['received', frame('CONNECTED', { version: '1.2' })]
]
const subscribe = (id, receipt = `${id}-sub_receipt`) => [
  ['sent', frame('SUBSCRIBE', { id, receipt, destination: TOPIC })],
  ['received', frame('RECEIPT', { 'receipt-id': receipt })]
]
const unsubscribe = id => [['sent', frame('UNSUBSCRIBE', { id, receipt: `${id}-unsub_receipt` })]]
let seq = 0
const message = (subscription, price) => {
  const body = JSON.stringify({ code: 'TEST', price })
  const headers = {
    'message-id': `c-${++seq}`,
    subscription,
    'content-length': body.length,
    'content-type': 'application/json'
  }
  return ['received', frame('MESSAGE', headers, body)]
}

// 같은 destination을 a와 b 두 id로 구독한 뒤 steps를 잇는다.
const deliveredToEach = (...steps) => {
  const contract = extract([...connect, ...subscribe('a'), ...subscribe('b'), ...steps.flat()])
  assert.equal(contract.server.duplicateSubscriptions.length, 1)
  assert.equal(contract.server.duplicateSubscriptions[0].subscriptions, 2)
  return contract.server.duplicateSubscriptions[0].deliveredToEach
}

test('두 구독이 같은 본문을 한 번씩 받으면 true', () => {
  assert.equal(deliveredToEach([message('a', 1), message('b', 1), message('b', 2), message('a', 2)]), true)
})

test('두 번째 구독이 하나도 받지 못하면 false', () => {
  assert.equal(deliveredToEach([message('a', 1)]), false)
})

test('두 번째 구독이 일부만 받으면 false', () => {
  assert.equal(deliveredToEach([message('a', 1), message('b', 1), message('a', 2)]), false)
})

test('한 구독이 같은 본문을 두 번 받으면 false', () => {
  assert.equal(deliveredToEach([message('a', 1), message('b', 1), message('b', 1)]), false)
})

test('함께 구독하던 구간에 메시지가 없으면 근거가 없으므로 null', () => {
  assert.equal(deliveredToEach([]), null)
})

test('한쪽만 구독하던 구간의 메시지는 견주지 않는다', () => {
  const contract = extract([
    ...connect,
    ...subscribe('a'),
    message('a', 1),
    ...subscribe('b'),
    message('a', 2),
    message('b', 2),
    ...unsubscribe('a'),
    message('b', 3)
  ])
  assert.equal(contract.server.duplicateSubscriptions[0].deliveredToEach, true)
})

test('receipt 값을 다시 써도 그 구독보다 뒤에 받은 RECEIPT부터 센다', () => {
  const contract = extract([
    ...connect,
    ...subscribe('a', 'r'),
    message('a', 1),
    ...subscribe('b', 'r'),
    message('a', 2),
    message('b', 2)
  ])
  assert.equal(contract.server.duplicateSubscriptions[0].deliveredToEach, true)
})

test('해지한 id를 다시 써도 그 구독보다 뒤에 보낸 UNSUBSCRIBE까지 센다', () => {
  const contract = extract([
    ...connect,
    ...subscribe('a'),
    ...unsubscribe('a'),
    ...subscribe('a', 'a-again'),
    ...subscribe('b'),
    message('a', 1),
    message('b', 1)
  ])
  assert.equal(contract.server.duplicateSubscriptions[0].deliveredToEach, true)
})

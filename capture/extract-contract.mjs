#!/usr/bin/env node
// 프레임 캡처(jsonl)에서 서버가 지켜야 할 모양만 뽑는다. 토큰과 실제 시세 값은 옮기지 않는다.
// 같은 추출기를 진짜 서버의 캡처와 직접 만든 서버의 캡처에 돌려 결과를 diff 한다.
// 사용: node capture/extract-contract.mjs evidence/raw/frames.jsonl > contract/wts-stomp.json
import { readFileSync } from 'node:fs'

const records = readFileSync(process.argv[2], 'utf8')
  .trim()
  .split('\n')
  .map((line, order) => ({ order, ...JSON.parse(line) }))

const REDACTED_HEADERS = new Set(['authorization', 'device-id'])
const VOCABULARY_KEYS = new Set(['currency', 'exchange', 'session', 'tradeType', 'changeType'])
const FAKE_CONNECTION_ID = '00000000-0000-4000-8000-000000000000'

function parseFrame(text) {
  if (text.replaceAll('\n', '') === '') return { heartbeat: true }
  const split = text.indexOf('\n\n')
  const [command, ...lines] = text.slice(0, split).split('\n')
  const headers = lines.map((line) => {
    const colon = line.indexOf(':')
    return [line.slice(0, colon), line.slice(colon + 1)]
  })
  return { command, headers, body: text.slice(split + 2).replace(/\0$/, ''), endsWithNul: text.endsWith('\0') }
}

const header = (frame, name) => frame.headers.find(([key]) => key === name)?.[1]
const unique = (values) => [...new Set(values.map((value) => JSON.stringify(value)))].map((value) => JSON.parse(value))
const gaps = (times) => times.slice(1).map((time, index) => time - times[index])

// order는 보낸 프레임과 받은 프레임을 통틀어 캡처에 적힌 순서다
const sent = records.filter((r) => r.event === 'webSocketFrameSent').map((r) => ({ ms: r.ms, order: r.order, ...parseFrame(r.response.payloadData) }))
const received = records
  .filter((r) => r.event === 'webSocketFrameReceived')
  .map((r, index) => ({ ms: r.ms, order: r.order, index, ...parseFrame(r.response.payloadData) }))
const request = records.find((r) => r.event === 'webSocketWillSendHandshakeRequest').request.headers
const response = records.find((r) => r.event === 'webSocketHandshakeResponseReceived').response
const created = records.find((r) => r.event === 'webSocketCreated')

const connect = sent.find((frame) => frame.command === 'CONNECT')
const connectionId = header(connect, 'connection-id')
const subscribes = sent.filter((frame) => frame.command === 'SUBSCRIBE')
const destinationOf = Object.fromEntries(subscribes.map((frame) => [header(frame, 'id'), header(frame, 'destination')]))
const pattern = (destination) => destination.replace(/[^/]+$/, '{code}')

const clientFrames = sent.map((frame) => {
  const atMs = frame.ms - connect.ms
  if (frame.heartbeat) return { atMs, heartbeat: true }
  const headers = frame.headers.map(([key, value]) => {
    if (REDACTED_HEADERS.has(key)) return [key, '<redacted>']
    if (key === 'connection-id') return [key, FAKE_CONNECTION_ID]
    return [key, value]
  })
  return { atMs, command: frame.command, headers }
})

const messages = received.filter((frame) => frame.command === 'MESSAGE')
const receipts = received.filter((frame) => frame.command === 'RECEIPT')
const unsubscribes = sent.filter((frame) => frame.command === 'UNSUBSCRIBE')
// 구독 확인과 해지 확인은 같은 RECEIPT 명령이라, 어느 요청의 receipt 값인지로 나눈다
const receiptShapes = (requests) => {
  const ids = new Set(requests.map((frame) => header(frame, 'receipt')))
  const matched = receipts.filter((frame) => ids.has(header(frame, 'receipt-id')))
  return unique(matched.map((frame) => frame.headers.map(([key, value]) => [key, key === 'receipt-id' ? '{receipt}' : value])))
}
const sequence = messages.map((frame) => header(frame, 'message-id'))

const bodies = {}
for (const frame of messages) {
  const shape = (bodies[pattern(destinationOf[header(frame, 'subscription')])] ??= { count: 0, keyOrders: [], fields: {} })
  shape.count++
  const keys = []
  for (const [, key, raw] of frame.body.matchAll(/"(\w+)":("(?:[^"\\]|\\.)*"|[^,}]+)/g)) {
    keys.push(key)
    const field = (shape.fields[key] ??= { types: [] })
    const type = raw.startsWith('"') ? 'string' : /^-?\d+$/.test(raw) ? 'integer' : /^-?\d/.test(raw) ? 'decimal' : raw
    if (!field.types.includes(type)) field.types.push(type)
    if (type === 'decimal') field.maxSignificantDigits = Math.max(field.maxSignificantDigits ?? 0, raw.replace(/^-?0*\.?0*|\./g, '').length)
    if (VOCABULARY_KEYS.has(key)) field.values = [...new Set([...(field.values ?? []), JSON.parse(raw)])].sort()
    if (key === 'dt') field.format = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/.test(JSON.parse(raw)) ? 'UTC, 초 단위' : 'unknown'
  }
  shape.keyOrders = unique([...shape.keyOrders, keys])
}

const perSecond = Object.values(Object.groupBy(messages, (frame) => Math.floor(frame.ms / 1000))).map((group) => group.length)
// 구독이 살아 있는 구간은 구독 RECEIPT를 받은 뒤부터 UNSUBSCRIBE를 보내기 전까지다. RECEIPT가 없으면 언제부터인지 알 수 없어 근거로 쓰지 않는다
// receipt 값이나 id가 다시 쓰여도 엉뚱한 프레임을 고르지 않게, 이 SUBSCRIBE보다 뒤에 오간 것 중 첫 번째를 고른다
const activeSpan = (frame) => {
  const id = header(frame, 'id')
  const receipt = receipts.find((r) => r.order > frame.order && header(r, 'receipt-id') === header(frame, 'receipt'))
  const unsubscribe = unsubscribes.find((u) => u.order > frame.order && header(u, 'id') === id)
  return { id, from: receipt?.order ?? Infinity, until: unsubscribe?.order ?? Infinity }
}
// 그 구간에 구독이 받은 본문과 횟수. 두 구독의 결과를 문자열로 견준다
const bodyCounts = (id, from, until) => {
  const counts = {}
  for (const frame of messages) {
    if (header(frame, 'subscription') === id && frame.order > from && frame.order < until) counts[frame.body] = (counts[frame.body] ?? 0) + 1
  }
  return JSON.stringify(Object.entries(counts).sort())
}
const duplicated = Object.entries(Object.groupBy(subscribes, (frame) => header(frame, 'destination')))
  .filter(([, group]) => group.length > 1)
  .map(([destination, group]) => {
    const spans = group.map(activeSpan)
    // 두 구독이 함께 살아 있던 구간마다 같은 본문을 같은 횟수만큼 받았는지 본다. 그 구간에 메시지가 없는 쌍은 근거가 못 된다
    const verdicts = spans.flatMap((a, i) =>
      spans.slice(i + 1).flatMap((b) => {
        const [from, until] = [Math.max(a.from, b.from), Math.min(a.until, b.until)]
        const [left, right] = [bodyCounts(a.id, from, until), bodyCounts(b.id, from, until)]
        return left === '[]' && right === '[]' ? [] : [left === right]
      }),
    )
    return { destination: pattern(destination), subscriptions: group.length, deliveredToEach: verdicts.length === 0 ? null : verdicts.every(Boolean) }
  })

console.log(
  JSON.stringify(
    {
      handshake: {
        path: new URL(created.url).pathname,
        offeredProtocols: request['Sec-WebSocket-Protocol'],
        selectedProtocol: response.headers['sec-websocket-protocol'],
        offeredExtensions: request['Sec-WebSocket-Extensions'] ?? null,
        selectedExtensions: response.headers['sec-websocket-extensions'] ?? null,
      },
      clientFrames,
      server: {
        opcodes: unique(records.filter((r) => r.event === 'webSocketFrameReceived').map((r) => r.response.opcode)),
        everyFrameEndsWithNul: received.filter((frame) => !frame.heartbeat).every((frame) => frame.endsWithNul),
        connected: received.find((frame) => frame.command === 'CONNECTED').headers,
        receipt: receiptShapes(subscribes),
        unsubscribeReceipt: receiptShapes(unsubscribes),
        noMessageAfterUnsubscribeReceipt:
          unsubscribes.length === 0
            ? null
            : unsubscribes.every((frame) => {
                const receipt = receipts.find((r) => header(r, 'receipt-id') === header(frame, 'receipt'))
                return receipt !== undefined && !messages.some((m) => header(m, 'subscription') === header(frame, 'id') && m.index > receipt.index)
              }),
        receiptBeforeFirstMessage: subscribes.every((frame) => {
          const receipt = receipts.find((r) => header(r, 'receipt-id') === header(frame, 'receipt'))
          const first = messages.find((m) => header(m, 'subscription') === header(frame, 'id'))
          return receipt !== undefined && (first === undefined || receipt.index < first.index)
        }),
        message: {
          headerOrders: unique(messages.map((frame) => frame.headers.map(([key]) => key))),
          messageId: '{connection-id}-{seq}',
          messageIdMatchesConnectionId: sequence.every((id) => id.startsWith(`${connectionId}-`)),
          sequenceStartsAtOneWithoutGaps: sequence.every((id, index) => id === `${connectionId}-${index + 1}`),
          contentLengthEqualsBodyBytes: messages.every((frame) => Number(header(frame, 'content-length')) === Buffer.byteLength(frame.body)),
          contentTypes: unique(messages.map((frame) => header(frame, 'content-type'))),
        },
        bodies,
        duplicateSubscriptions: duplicated,
      },
      timing: {
        serverHeartbeatGapsMs: gaps(received.filter((frame) => frame.heartbeat).map((frame) => frame.ms)),
        clientHeartbeatGapsMs: gaps(sent.filter((frame) => frame.heartbeat).map((frame) => frame.ms)),
        messagesPerSecond: { min: Math.min(...perSecond), max: Math.max(...perSecond) },
      },
    },
    null,
    2,
  ),
)

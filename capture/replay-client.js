#!/usr/bin/env node
// 계약 파일의 clientFrames(실제 화면이 보낸 프레임)를 같은 시간 간격으로 다시 보내고,
// 오간 프레임을 cdp-capture.js와 같은 jsonl 형식으로 적는다. 결과에 extract-contract.js를 돌리면
// 직접 만든 서버의 계약이 나오고, 그것을 진짜 서버의 계약과 diff 한다.
// 사용: node capture/replay-client.js ws://127.0.0.1:8080/ws [--contract contract/wts-stomp.json] [--seconds 25] [--out evidence/raw/replay.jsonl]
import { readFileSync, writeFileSync } from 'node:fs'
import { parseArgs } from 'node:util'

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    contract: { type: 'string', default: 'contract/wts-stomp.json' },
    seconds: { type: 'string', default: '25' },
    out: { type: 'string', default: 'evidence/raw/replay.jsonl' }
  }
})
const url = positionals[0]
if (!url) {
  console.error('usage: node capture/replay-client.js <ws url> [--contract file] [--seconds 25] [--out file.jsonl]')
  process.exit(1)
}

const contract = JSON.parse(readFileSync(values.contract, 'utf8'))
const records = []
const startedAt = Date.now()
const record = (event, fields) => records.push({ ms: Date.now() - startedAt, event, ...fields })

const serialize = frame =>
  frame.heartbeat ? '\n' : `${frame.command}\n${frame.headers.map(([key, value]) => `${key}:${value}\n`).join('')}\n\0`

const socket = new WebSocket(url, contract.handshake.offeredProtocols.split(', '))
record('webSocketCreated', { url })
// Node 내장 WebSocket은 자신이 제안한 확장을 알려 주지 않으므로 요청 쪽 확장은 적지 않는다
record('webSocketWillSendHandshakeRequest', {
  request: { headers: { 'Sec-WebSocket-Protocol': contract.handshake.offeredProtocols } }
})

socket.onmessage = ({ data }) =>
  record('webSocketFrameReceived', {
    response: { opcode: typeof data === 'string' ? 1 : 2, mask: false, payloadData: String(data) }
  })
socket.onclose = ({ code, reason }) => record('webSocketClosed', { code, reason })
socket.onerror = () => {
  console.error(`연결 실패: ${url}`)
  process.exit(1)
}
socket.onopen = () => {
  const headers = { 'sec-websocket-protocol': socket.protocol }
  if (socket.extensions) headers['sec-websocket-extensions'] = socket.extensions
  record('webSocketHandshakeResponseReceived', { response: { status: 101, headers } })
  for (const frame of contract.clientFrames) {
    setTimeout(() => {
      if (socket.readyState !== WebSocket.OPEN) return
      const payloadData = serialize(frame)
      socket.send(payloadData)
      record('webSocketFrameSent', { response: { opcode: 1, mask: true, payloadData } })
    }, frame.atMs)
  }
}

setTimeout(
  () => {
    socket.close()
    writeFileSync(values.out, records.map(entry => JSON.stringify(entry)).join('\n') + '\n')
    const count = event => records.filter(entry => entry.event === event).length
    console.log(
      `sent ${count('webSocketFrameSent')}, received ${count('webSocketFrameReceived')}, closed ${count('webSocketClosed')} -> ${values.out}`
    )
    process.exit(0)
  },
  Number(values.seconds) * 1000
)

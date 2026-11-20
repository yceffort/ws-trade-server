import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { LatestState } from './latest-state.mjs'
import { recover } from './recover.mjs'

const [url = 'ws://127.0.0.1:8080/ws', http = 'http://127.0.0.1:8081/snapshots', ...topics] = process.argv.slice(2)
if (!topics.length) throw new Error('하나 이상의 topic이 필요하다')
const state = new LatestState()
let attempts = 0
const frame = (command, headers) => `${command}\n${Object.entries(headers).map(([k, v]) => `${k}:${v}`).join('\n')}\n\n\0`
for (;;) {
  const started = Date.now()
  await new Promise((resolve) => {
    const socket = new WebSocket(url, 'v12.stomp')
    const abort = new AbortController()
    let heartbeat
    socket.onopen = () => socket.send(frame('CONNECT', { 'connection-id': randomUUID(), ...(process.env.DEMO_TOKEN ? { authorization: `Bearer ${process.env.DEMO_TOKEN}` } : {}) }))
    socket.onmessage = ({ data }) => {
      const [head, body = ''] = String(data).split('\n\n')
      const [command, ...lines] = head.split('\n')
      const headers = Object.fromEntries(lines.map((line) => { const at = line.indexOf(':'); return [line.slice(0, at), line.slice(at + 1)] }))
      if (command === 'CONNECTED') {
        topics.forEach((topic, i) => socket.send(frame('SUBSCRIBE', { id: String(i), destination: topic, receipt: String(i) })))
        heartbeat = setInterval(() => { if (socket.readyState === WebSocket.OPEN) socket.send('\n') }, 5000)
      }
      if (command === 'RECEIPT' && headers['receipt-id'] === String(topics.length - 1)) {
        recover(state, http, abort.signal).then((restored) => { if (restored) console.log(Object.fromEntries(state.values)) })
      }
      if (command === 'MESSAGE') {
        const topic = topics[Number(headers.subscription)]
        state.stream(topic, body.replace(/\0$/, ''))
        console.log(topic, state.values.get(topic))
      }
      if (command === 'ERROR') { console.error('서버 종료 사유:', headers.message); socket.close() }
    }
    socket.onerror = () => socket.close()
    socket.onclose = () => { clearInterval(heartbeat); abort.abort(); resolve() }
  })
  attempts = Date.now() - started > 30000 ? 0 : Math.min(attempts + 1, 6)
  await sleep(Math.random() * Math.min(30000, 500 * 2 ** attempts))
}

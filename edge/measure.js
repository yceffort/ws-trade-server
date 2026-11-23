import WebSocket from 'ws'
import { once } from 'node:events'
import { readFileSync } from 'node:fs'
const [url = 'ws://127.0.0.1:8080/ws', ca] = process.argv.slice(2)
const start = performance.now()
const socket = new WebSocket(url, 'v12.stomp', {
  ca: ca ? readFileSync(ca) : undefined,
  perMessageDeflate: false,
  handshakeTimeout: 5000
})
await once(socket, 'open')
const next = async command => {
  for (;;) {
    const [data] = await once(socket, 'message', { signal: AbortSignal.timeout(5000) })
    const text = data.toString()
    if (text.startsWith('ERROR')) throw new Error(text.split('\n')[1])
    if (text.startsWith(command + '\n')) return
  }
}
const connected = next('CONNECTED')
socket.send('CONNECT\nconnection-id:measure\n\n\0')
await connected
const connect_ms = performance.now() - start
const samples = []
for (let i = 0; i < 100; i++) {
  const sent = performance.now(),
    receipt = next('RECEIPT')
  socket.send(`SUBSCRIBE\nid:1\ndestination:/topic/demo\nreceipt:${i}\n\n\0`)
  await receipt
  samples.push(performance.now() - sent)
}
samples.sort((a, b) => a - b)
console.log(JSON.stringify({ connect_ms, receipt_p50_ms: samples[49], receipt_p99_ms: samples[98] }))
socket.close()

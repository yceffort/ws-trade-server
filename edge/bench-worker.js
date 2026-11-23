import { once } from 'node:events'
import { createHash } from 'node:crypto'
import WebSocket from 'ws'
import { startProxy } from './proxy.js'
import { echoServer } from './fixtures.js'

const compression = process.argv[2] === 'deflate'
const count = Number(process.argv[3] ?? 1000)
if (!Number.isInteger(count) || count < 100 || count > 100000) throw new Error('건수는 100 이상 100000 이하 정수')
const backend = await echoServer()
const proxy = await startProxy({ backend: backend.url, compression })
const socket = new WebSocket(`ws://127.0.0.1:${proxy.address.port}/ws`, 'v12.stomp', { perMessageDeflate: compression ? { threshold: 0 } : false })
let transport
socket.on('upgrade', (response) => { transport = response.socket })
await once(socket, 'open')
const before = { read: transport.bytesRead, written: transport.bytesWritten }
const cpu = process.cpuUsage(), started = performance.now(), hash = createHash('sha256'), samples = []
let bodyBytes = 0
try {
  for (let i = 0; i < count; i++) {
    const body = `MESSAGE\nsubscription:demo\nmessage-id:synthetic-${i}\n\n${JSON.stringify({ code: 'DEMO', price: 12345 + i % 100, volume: i % 10 + 1, description: 'synthetic quote '.repeat(20) })}\0`
    hash.update(body); bodyBytes += Buffer.byteLength(body)
    const sent = performance.now(), received = once(socket, 'message', { signal: AbortSignal.timeout(5000) })
    socket.send(body)
    if ((await received)[0].toString() !== body) throw new Error('본문 불일치')
    samples.push(performance.now() - sent)
  }
  const elapsed_ms = performance.now() - started, used = process.cpuUsage(cpu)
  samples.sort((a, b) => a - b)
  console.log(JSON.stringify({ mode: compression ? 'deflate' : 'plain', count, dataset_sha256: hash.digest('hex'), negotiated: socket.extensions, body_bytes_one_way: bodyBytes, ws_bytes_read: transport.bytesRead - before.read, ws_bytes_written: transport.bytesWritten - before.written, cpu_user_us: used.user, cpu_system_us: used.system, max_rss_kib: process.resourceUsage().maxRSS, elapsed_ms, rtt_p50_ms: samples[Math.ceil(count * .5) - 1], rtt_p99_ms: samples[Math.ceil(count * .99) - 1] }))
} finally {
  socket.terminate(); await proxy.close(); await backend.close()
}

// edge 하나가 받을 수 있는 연결 수를 잰다(#95). 합성 upstream이 연결마다 10ms마다 450바이트 프레임을 보내고,
// 가벼운 수신 클라이언트가 받은 수만 센다. 클라이언트와 upstream이 같은 호스트에 있으므로 edge만 포화되는 범위에서 읽는다.
// 사용: WTS_EDGE_WORKERS=2 node edge/capacity.mjs 600,1000,1400 [초=20] [수신 스레드=4] [스레드당 접속 간격 ms=20]
import { spawn, execFileSync } from 'node:child_process'
import { once } from 'node:events'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Worker } from 'node:worker_threads'
import { WebSocketServer } from 'ws'
import { certificate } from './fixtures.mjs'

const [steps = '200,400,600,800,1000', seconds = '20', threads = '4', pace = '20'] = process.argv.slice(2)
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const frame = JSON.stringify({ type: 'event', event_id: 'demo/trades/epoch/1', topic: '/topic/v1/kr/stock/trade/A005930', position: { log_id: 'demo', partition: 0, offset: '1' }, body: { padding: 'x'.repeat(300) } })
const backend = new WebSocketServer({ host: '127.0.0.1', port: 0, handleProtocols: () => 'wts.ha.v1' })
await once(backend, 'listening')
let sent = 0
const timer = setInterval(() => { for (const socket of backend.clients) { socket.send(frame); sent++ } }, 10)
const { tls, close } = certificate()
const dir = mkdtempSync(join(tmpdir(), 'wts-edge-capacity-'))
writeFileSync(join(dir, 'cert.pem'), tls.cert); writeFileSync(join(dir, 'key.pem'), tls.key)
const edge = spawn(process.execPath, [fileURLToPath(new URL('run.mjs', import.meta.url)), `ws://127.0.0.1:${backend.address().port}`, '0', join(dir, 'cert.pem'), join(dir, 'key.pem')], { stdio: ['ignore', 'ignore', 'pipe'] })
let log = ''
edge.stderr.on('data', (data) => { log += data })
while (!/edge_started/.test(log)) await sleep(50)
const url = `wss://127.0.0.1:${log.match(/wss:\/\/127\.0\.0\.1:(\d+)/)[1]}/experimental/ha/ws`
// cluster worker까지 합친 누적 CPU 초와 RSS(MiB)다. macOS의 RSS 합은 공유 페이지를 겹쳐 센다.
const usage = () => {
  const rows = execFileSync('ps', ['-A', '-o', 'pid=,ppid=,time=,rss='], { encoding: 'utf8' }).trim().split('\n').map((line) => line.trim().split(/\s+/))
  const mine = rows.filter(([pid, ppid]) => Number(pid) === edge.pid || Number(ppid) === edge.pid)
  const cpu = mine.reduce((total, [, , time]) => total + time.split(':').reduce((a, part) => a * 60 + Number(part), 0), 0)
  return { cpu, rss_mib: Math.round(mine.reduce((total, row) => total + Number(row[3]), 0) / 1024) }
}
// 수신 스레드는 eval worker라 CommonJS로 쓰고, edge가 쓰는 ws를 같은 경로로 불러온다.
const client = `const { workerData, parentPort } = require('node:worker_threads')
const WebSocket = require(workerData.ws)
const { url, count, pace, ca } = workerData
let received = 0, open = 0
const failed = {}
parentPort.on('message', () => { parentPort.postMessage({ received, failed, open }); received = 0 })
;(async () => {
  for (let i = 0; i < count; i++) {
    const socket = new WebSocket(url, 'wts.ha.v1', { ca, perMessageDeflate: false, handshakeTimeout: 10000 })
    socket.on('open', () => open++)
    socket.on('message', () => received++)
    socket.on('error', (error) => { const reason = error.code ?? 'error'; failed[reason] = (failed[reason] ?? 0) + 1 })
    await new Promise((resolve) => setTimeout(resolve, pace))
  }
})()`
const ws = createRequire(import.meta.url).resolve('ws')
try {
  for (const clients of steps.split(',').map(Number)) {
    const pool = Array.from({ length: Number(threads) }, (_, i) => new Worker(client, { eval: true, workerData: { url, ws, ca: tls.cert, pace: Number(pace), count: Math.floor(clients / threads) + (i < clients % threads ? 1 : 0) } }))
    const ask = () => Promise.all(pool.map((worker) => new Promise((resolve) => { worker.once('message', resolve); worker.postMessage(0) })))
    await sleep(Math.max(8000, (clients / threads) * pace + 5000))
    await ask()
    const before = usage(), start = sent, started = performance.now()
    await sleep(Number(seconds) * 1000)
    const replies = await ask(), after = usage(), elapsed = (performance.now() - started) / 1000
    const failed = {}
    for (const reply of replies) for (const [reason, n] of Object.entries(reply.failed)) failed[reason] = (failed[reason] ?? 0) + n
    const received = replies.reduce((total, reply) => total + reply.received, 0)
    console.log(JSON.stringify({ clients, workers: Number(process.env.WTS_EDGE_WORKERS || 1), open: replies.reduce((total, reply) => total + reply.open, 0), sent_per_s: Math.round((sent - start) / elapsed), delivered_per_s: Math.round(received / elapsed), edge_cores: Number(((after.cpu - before.cpu) / elapsed).toFixed(2)), rss_mib: after.rss_mib, failed }))
    await Promise.all(pool.map((worker) => worker.terminate()))
    await sleep(3000)
  }
} finally {
  clearInterval(timer); edge.kill('SIGTERM'); await once(edge, 'exit'); backend.close(); close(); rmSync(dir, { recursive: true, force: true })
}

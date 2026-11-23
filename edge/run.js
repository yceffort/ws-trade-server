import cluster from 'node:cluster'
import { readFileSync } from 'node:fs'
import { startProxy } from './proxy.js'
const env = process.env
// gateway JSON 로그와 같은 모양(timestamp, level, target, fields.event)으로 stderr에 한 줄씩 쓴다.
const log = (level, event, fields = {}) =>
  process.stderr.write(`${JSON.stringify({ timestamp: new Date().toISOString(), level, target: 'edge', fields: { event, ...(cluster.isWorker ? { worker: cluster.worker.id } : {}), ...fields } })}\n`)
const workers = Number(env.WTS_EDGE_WORKERS || 1)
if (!Number.isInteger(workers) || workers < 1 || workers > 16) throw new Error('WTS_EDGE_WORKERS는 1 이상 16 이하 정수다')
if (workers > 1 && cluster.isPrimary) {
  // TLS handshake와 중계가 한 스레드에 묶이지 않게 worker 여러 개가 같은 포트를 나눠 받는다(#95).
  let closing = false
  const stop = () => { closing = true; for (const worker of Object.values(cluster.workers)) worker.process.kill('SIGTERM') }
  for (let i = 0; i < workers; i++) cluster.fork()
  cluster.on('listening', (worker) => { worker.ready = true })
  cluster.on('exit', (worker, code, signal) => {
    if (!closing && worker.ready) {
      // 한 worker가 죽어도 다른 worker의 연결은 유지하고 그 자리만 다시 띄운다.
      log('WARN', 'edge_worker_exited', { code, signal })
      cluster.fork()
    } else if (!closing) {
      // 듣기도 전에 죽으면 설정이나 포트 문제라 다시 띄워도 같다. 전체를 실패로 끝낸다.
      process.exitCode = 1
      stop()
    }
    if (closing && !Object.keys(cluster.workers).length) log(process.exitCode ? 'ERROR' : 'INFO', 'edge_stopped')
  })
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { if (!closing) stop() })
} else {
  const [backend = env.WTS_BACKEND ?? 'ws://127.0.0.1:8080/ws', port = env.WTS_EDGE_PORT ?? '8443',
    cert = env.WTS_TLS_CERT_FILE, key = env.WTS_TLS_KEY_FILE, compression = env.WTS_COMPRESSION] = process.argv.slice(2)
  const host = env.WTS_EDGE_HOST || '127.0.0.1'
  if (Boolean(cert) !== Boolean(key)) throw new Error('인증서와 키 경로를 함께 지정한다')
  const proxy = await startProxy({ backend, host, port: Number(port), compression: compression === 'deflate', tls: cert ? { cert: readFileSync(cert), key: readFileSync(key) } : undefined })
  log('INFO', 'edge_started', { url: `${cert ? 'wss' : 'ws'}://${host}:${proxy.address.port}/ws` })
  // 연결마다 남기지 않고 10초마다 사유별 증분만 낸다. 정상 종료가 아닌 사유가 있으면 WARN이다.
  const normal = new Set(['client_closed', 'upstream_closed', 'shutdown', 'tls_aborted'])
  let reported = proxy.counts()
  const summarize = () => {
    const now = proxy.counts()
    const delta = Object.fromEntries(Object.entries(now).map(([reason, n]) => [reason, n - reported[reason]]).filter(([, n]) => n > 0))
    reported = now
    if (Object.keys(delta).length) log(Object.keys(delta).some((reason) => !normal.has(reason)) ? 'WARN' : 'INFO', 'edge_summary', delta)
  }
  setInterval(summarize, 10000).unref()
  let closing = false
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => {
    if (closing) return
    closing = true
    await proxy.close()
    summarize()
    log('INFO', 'edge_stopped')
    // worker는 primary와의 IPC가 남아 있으면 끝나지 않는다.
    cluster.worker?.disconnect()
  })
}

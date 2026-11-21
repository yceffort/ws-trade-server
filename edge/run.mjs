import { readFileSync } from 'node:fs'
import { startProxy } from './proxy.mjs'
const env = process.env
const [backend = env.WTS_BACKEND ?? 'ws://127.0.0.1:8080/ws', port = env.WTS_EDGE_PORT ?? '8443',
  cert = env.WTS_TLS_CERT_FILE, key = env.WTS_TLS_KEY_FILE, compression = env.WTS_COMPRESSION] = process.argv.slice(2)
const host = env.WTS_EDGE_HOST || '127.0.0.1'
if (Boolean(cert) !== Boolean(key)) throw new Error('인증서와 키 경로를 함께 지정한다')
const proxy = await startProxy({ backend, host, port: Number(port), compression: compression === 'deflate', tls: cert ? { cert: readFileSync(cert), key: readFileSync(key) } : undefined })
// gateway JSON 로그와 같은 모양(timestamp, level, target, fields.event)으로 stderr에 한 줄씩 쓴다.
const log = (level, event, fields = {}) =>
  process.stderr.write(`${JSON.stringify({ timestamp: new Date().toISOString(), level, target: 'edge', fields: { event, ...fields } })}\n`)
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
})

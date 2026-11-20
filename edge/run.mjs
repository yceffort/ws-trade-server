import { readFileSync } from 'node:fs'
import { startProxy } from './proxy.mjs'
const env = process.env
const [backend = env.WTS_BACKEND ?? 'ws://127.0.0.1:8080/ws', port = env.WTS_EDGE_PORT ?? '8443',
  cert = env.WTS_TLS_CERT_FILE, key = env.WTS_TLS_KEY_FILE, compression = env.WTS_COMPRESSION] = process.argv.slice(2)
const host = env.WTS_EDGE_HOST || '127.0.0.1'
if (Boolean(cert) !== Boolean(key)) throw new Error('인증서와 키 경로를 함께 지정한다')
const proxy = await startProxy({ backend, host, port: Number(port), compression: compression === 'deflate', tls: cert ? { cert: readFileSync(cert), key: readFileSync(key) } : undefined })
console.error(`${cert ? 'wss' : 'ws'}://${host}:${proxy.address.port}/ws`)
let closing = false
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => { if (!closing) { closing = true; await proxy.close() } })

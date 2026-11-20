import { readFileSync } from 'node:fs'
import { startProxy } from './proxy.mjs'
const [backend = 'ws://127.0.0.1:8080/ws', port = '8443', cert, key] = process.argv.slice(2)
if (Boolean(cert) !== Boolean(key)) throw new Error('인증서와 키 경로를 함께 지정한다')
const proxy = await startProxy({ backend, port: Number(port), tls: cert ? { cert: readFileSync(cert), key: readFileSync(key) } : undefined })
console.error(`${cert ? 'wss' : 'ws'}://127.0.0.1:${proxy.address.port}/ws`)
let closing = false
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => { if (!closing) { closing = true; await proxy.close() } })

import { execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { once } from 'node:events'
import { WebSocketServer } from 'ws'

export function certificate() {
  const dir = mkdtempSync(join(tmpdir(), 'ws-trade-tls-'))
  const key = join(dir, 'key.pem'),
    cert = join(dir, 'cert.pem')
  execFileSync(
    'openssl',
    [
      'req',
      '-x509',
      '-newkey',
      'rsa:2048',
      '-noenc',
      '-days',
      '1',
      '-subj',
      '/CN=localhost',
      '-addext',
      'subjectAltName=DNS:localhost,IP:127.0.0.1',
      '-keyout',
      key,
      '-out',
      cert
    ],
    { stdio: 'ignore' }
  )
  return {
    tls: { key: readFileSync(key), cert: readFileSync(cert) },
    close: () => rmSync(dir, { recursive: true, force: true })
  }
}

export async function echoServer() {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0, handleProtocols: () => 'v12.stomp' })
  server.on('connection', socket => socket.on('message', (data, binary) => socket.send(data, { binary })))
  await once(server, 'listening')
  return {
    url: `ws://127.0.0.1:${server.address().port}/ws`,
    server,
    async close() {
      for (const client of server.clients) client.terminate()
      await new Promise(resolve => server.close(resolve))
    }
  }
}

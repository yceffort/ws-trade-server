import { createServer } from 'node:https'
import { readFileSync, writeFileSync, renameSync } from 'node:fs'
import { join } from 'node:path'
import { execFileSync } from 'node:child_process'

const directory = process.argv[2]
try {
  const key = join(directory, 'server.key'),
    cert = join(directory, 'server.crt'),
    ca = join(directory, 'ca.crt'),
    caKey = join(directory, 'ca.key'),
    csr = join(directory, 'server.csr')
  const openssl = args => execFileSync('openssl', args, { stdio: 'ignore' })
  openssl([
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-days',
    '1',
    '-subj',
    '/CN=policy-test-ca',
    '-addext',
    'basicConstraints=critical,CA:TRUE',
    '-keyout',
    caKey,
    '-out',
    ca
  ])
  openssl(['req', '-new', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost', '-keyout', key, '-out', csr])
  writeFileSync(
    join(directory, 'server.ext'),
    'basicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n'
  )
  openssl([
    'x509',
    '-req',
    '-in',
    csr,
    '-CA',
    ca,
    '-CAkey',
    caKey,
    '-CAcreateserial',
    '-days',
    '1',
    '-extfile',
    join(directory, 'server.ext'),
    '-out',
    cert
  ])
  const server = createServer({ key: readFileSync(key), cert: readFileSync(cert) }, (request, response) => {
    try {
      if (request.method !== 'GET' || request.url !== '/policy') {
        response.writeHead(404).end()
        return
      }
      const bytes = readFileSync(join(directory, 'control.json'))
      if (bytes.length > 1024 * 1024) throw new Error('size')
      const control = JSON.parse(bytes)
      if (control.failure) {
        response.writeHead(503).end()
        return
      }
      if (control.slow) return
      const now = Math.floor(Date.now() / 1000)
      response.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' })
      response.end(JSON.stringify({ generated_at: now, valid_until: now + 55, policy: control.policy }))
    } catch {
      response.writeHead(500).end()
    }
  })
  server.headersTimeout = 2000
  server.requestTimeout = 3000
  server.listen(0, '127.0.0.1', () => {
    writeFileSync(
      join(directory, 'ready.next'),
      JSON.stringify({ url: `https://localhost:${server.address().port}/policy`, ca_file: ca })
    )
    renameSync(join(directory, 'ready.next'), join(directory, 'ready.json'))
  })
  process.once('SIGTERM', () => {
    server.closeAllConnections()
    server.close()
  })
} catch {
  console.error('policy fixture failed')
  process.exitCode = 1
}

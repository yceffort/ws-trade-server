import { createServer } from 'node:http'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const token = readFileSync(fileURLToPath(new URL('../local/ha/lease-token', import.meta.url)), 'utf8').trim()
const port = Number(process.env.WTS_LEASE_PORT ?? '18081')
if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('invalid port')
let version = 1
let document = {
  apiVersion: 'coordination.k8s.io/v1',
  kind: 'Lease',
  metadata: { name: 'feed', namespace: 'wts', resourceVersion: '1' },
  spec: { holderIdentity: null, leaseDurationSeconds: 10, leaseTransitions: 0 }
}
// 로컬 장애 시험용 CAS 서버다. 운영 배포는 Kubernetes API의 Lease와 RBAC를 사용한다.
const server = createServer(async (request, response) => {
  const reply = (status, value = {}) => {
    response.writeHead(status, { 'content-type': 'application/json' })
    response.end(JSON.stringify(value))
  }
  if (request.headers.authorization !== `Bearer ${token}`) return reply(403)
  if (request.url !== '/apis/coordination.k8s.io/v1/namespaces/wts/leases/feed') return reply(404)
  if (request.method === 'GET') return reply(200, document)
  if (request.method !== 'PUT') return reply(405)
  try {
    const chunks = []
    let size = 0
    for await (const chunk of request) {
      size += chunk.length
      if (size > 65536) return reply(413)
      chunks.push(chunk)
    }
    const next = JSON.parse(Buffer.concat(chunks).toString())
    if (next.metadata?.resourceVersion !== String(version)) return reply(409)
    if (!next.spec || typeof next.spec.holderIdentity !== 'string' || !Number.isInteger(next.spec.leaseDurationSeconds))
      return reply(400)
    if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/.test(next.spec.renewTime)) return reply(400)
    document = { ...next, metadata: { ...next.metadata, resourceVersion: String(++version) } }
    reply(200, document)
  } catch {
    reply(400)
  }
})
server.listen(port, '127.0.0.1', () =>
  console.log(JSON.stringify({ event: 'lease_api_started', port: server.address().port }))
)

import { existsSync, mkdirSync, readFileSync, writeFileSync, chmodSync, renameSync, rmSync } from 'node:fs'
import { randomBytes, randomUUID } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { join, resolve } from 'node:path'

const destination = fileURLToPath(new URL('../local/ha', import.meta.url))
function prepareLease(directory) {
  const token = join(directory, 'lease-token')
  if (!existsSync(token)) writeFileSync(token, randomBytes(32).toString('hex'), { mode: 0o600 })
  for (const identity of ['demo-a', 'demo-b']) {
    const path = join(directory, `lease-${identity}.json`)
    if (!existsSync(path)) writeFileSync(path, JSON.stringify({ url: 'http://127.0.0.1:18081/apis/coordination.k8s.io/v1/namespaces/wts/leases/feed', token_file: join(destination, 'lease-token'), ca_file: null, identity, duration_seconds: 10, development_loopback_http: true }, null, 2), { mode: 0o600 })
  }
}
if (existsSync(join(destination, 'ready'))) {
  chmodSync(destination, 0o700)
  prepareLease(destination)
  console.log('기존 HA 개발 인증 자료를 사용합니다.')
  process.exit(0)
}
if (existsSync(destination)) throw new Error('불완전한 준비 디렉터리가 있습니다. 사용 여부를 확인한 뒤 정리하세요.')
const staging = `${destination}.${randomUUID()}`
mkdirSync(join(staging, 'private'), { recursive: true, mode: 0o700 })
chmodSync(staging, 0o700)
mkdirSync(join(staging, 'shared'), { mode: 0o755 })
const shared = join(staging, 'shared')
const privateDir = join(staging, 'private')
const openssl = (...args) => execFileSync('openssl', args, { stdio: 'ignore' })
const put = (name, text, mode = 0o444) => writeFileSync(join(shared, name), text, { mode })
try {
  openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '7', '-subj', '/CN=wts-dev-ca', '-keyout', join(privateDir, 'ca.key'), '-out', join(shared, 'ca.crt'))
  chmodSync(join(shared, 'ca.crt'), 0o444)
  const passwords = Object.fromEntries(['admin', 'broker', 'feed', 'gateway'].map(role => [role, randomBytes(24).toString('hex')]))
  put('cluster-id', `${randomBytes(16).toString('base64url')}\n`)
  for (let id = 1; id <= 3; id++) {
    const key = join(privateDir, `broker${id}.key`)
    const csr = join(privateDir, `broker${id}.csr`)
    const cert = join(privateDir, `broker${id}.crt`)
    const ext = join(privateDir, `broker${id}.ext`)
    writeFileSync(ext, `subjectAltName=DNS:kafka${id},DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n`)
    openssl('req', '-new', '-newkey', 'rsa:2048', '-nodes', '-subj', `/CN=wts-broker-${id}`, '-keyout', key, '-out', csr)
    openssl('x509', '-req', '-in', csr, '-CA', join(shared, 'ca.crt'), '-CAkey', join(privateDir, 'ca.key'), '-CAcreateserial', '-days', '7', '-extfile', ext, '-out', cert)
    put(`broker${id}.pem`, `${readFileSync(key, 'utf8')}\n${readFileSync(cert, 'utf8')}`)
    const jaas = `org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="${passwords.broker}" ${Object.entries(passwords).map(([role, password]) => `user_${role}="${password}"`).join(' ')};`
    const properties = {
      'node.id': id, 'process.roles': 'broker,controller',
      'controller.quorum.voters': '1@kafka1:9093,2@kafka2:9093,3@kafka3:9093',
      'controller.listener.names': 'CONTROLLER', 'inter.broker.listener.name': 'INTERNAL',
      'listeners': 'INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093',
      'advertised.listeners': `INTERNAL://kafka${id}:9092,EXTERNAL://127.0.0.1:${29090 + id}`,
      'listener.security.protocol.map': 'INTERNAL:SASL_SSL,EXTERNAL:SASL_SSL,CONTROLLER:SSL',
      'ssl.keystore.type': 'PEM', 'ssl.keystore.location': `/config/broker${id}.pem`,
      'ssl.truststore.type': 'PEM', 'ssl.truststore.location': '/config/ca.crt',
      'listener.name.controller.ssl.client.auth': 'required',
      'sasl.enabled.mechanisms': 'PLAIN', 'sasl.mechanism.inter.broker.protocol': 'PLAIN',
      'listener.name.internal.plain.sasl.jaas.config': jaas, 'listener.name.external.plain.sasl.jaas.config': jaas,
      'authorizer.class.name': 'org.apache.kafka.metadata.authorizer.StandardAuthorizer',
      'super.users': 'User:admin;User:broker;User:CN=wts-broker-1;User:CN=wts-broker-2;User:CN=wts-broker-3',
      'allow.everyone.if.no.acl.found': 'false', 'auto.create.topics.enable': 'false',
      'log.dirs': '/var/lib/kafka/data', 'default.replication.factor': 3, 'min.insync.replicas': 2,
      'offsets.topic.replication.factor': 3, 'offsets.topic.num.partitions': 1,
      'transaction.state.log.replication.factor': 3, 'transaction.state.log.min.isr': 2, 'transaction.state.log.num.partitions': 1,
      'unclean.leader.election.enable': 'false', 'log.segment.bytes': 16777216, 'log.cleaner.min.cleanable.ratio': 0.1,
    }
    put(`broker${id}.properties`, Object.entries(properties).map(([k, v]) => `${k}=${v}\n`).join(''))
  }
  for (const role of ['admin', 'feed', 'gateway']) {
    put(`${role}.properties`, `security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${role}" password="${passwords[role]}";\nssl.truststore.type=PEM\nssl.truststore.location=/config/ca.crt\n`)
    const kafka = { timeout_ms: 10000, properties: { 'bootstrap.servers': '127.0.0.1:29091,127.0.0.1:29092,127.0.0.1:29093', 'security.protocol': 'SASL_SSL', 'sasl.mechanism': 'PLAIN', 'sasl.username': role, 'sasl.password': passwords[role], 'ssl.ca.location': resolve(destination, 'shared/ca.crt') } }
    writeFileSync(join(staging, `${role}.json`), JSON.stringify(kafka, null, 2), { mode: 0o600 })
  }
  writeFileSync(join(staging, 'source.json'), JSON.stringify({ log_id: 'demo-v1', source_id: { provider: 'demo', stream: 'trades' }, epoch: 'epoch-1', start_seq: '1', topics: ['/topic/v1/kr/stock/trade/A005930', '/topic/v1/us/stock/trade/US19990122001'] }, null, 2))
  prepareLease(staging)
  writeFileSync(join(staging, 'ready'), '1\n')
  renameSync(staging, destination)
  console.log('HA 개발용 CA, broker 인증서와 역할별 설정을 생성했습니다.')
} catch (error) {
  rmSync(staging, { recursive: true, force: true })
  throw error
}

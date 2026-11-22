import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as sleep } from 'node:timers/promises'

const directory = mkdtempSync(join(tmpdir(), 'wts-alerts-'))
const source = fileURLToPath(new URL('.', import.meta.url))
const network = `wts-alert-check-${process.pid}`
const names = []
const docker = (...args) => execFileSync('docker', args, { encoding: 'utf8', timeout: 120000 }).trim()
const node = 'node:24-alpine@sha256:116b800c3754b440ee0aa3ed2f20f5bf3d99a554bfd9adeea09ea8017f6e85f5'
const prom = 'prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996'
async function wait(check, label, timeout = 60000) {
  const end = Date.now() + timeout
  while (Date.now() < end) { if (await check().catch(() => false)) return; await sleep(500) }
  throw new Error(label)
}
function start(name, port, image, ...args) {
  const full = `${network}-${name}`; names.push(full)
  docker('run', '-d', '--name', full, '--network', network, '--network-alias', name,
    ...(port ? ['-p', `127.0.0.1::${port}`] : []), '-v', `${source}:/rules:ro`, '-v', `${directory}:/config:ro`, image, ...args)
  return port ? `http://127.0.0.1:${docker('port', full, String(port)).split(':').at(-1)}` : null
}
try {
  docker('network', 'create', network)
  writeFileSync(join(directory, 'fixture.cjs'), `let age=0;require('node:http').createServer((q,r)=>{if(q.url==='/fault')age=4;if(q.url==='/recover')age=0;r.setHeader('content-type','text/plain; version=0.0.4');r.end('wts_ha_gateway_ready 1\\nwts_ha_source_age_seconds '+age+'\\nwts_ha_broker_age_seconds 0\\nwts_ha_policy_fresh 1\\n')}).listen(8080)`)
  const fixture = start('fixture', 8080, node, '/config/fixture.cjs')
  const receiver = start('alert-receiver', 8080, node, '/rules/alert-receiver.mjs')
  start('alertmanager', 9093, 'prom/alertmanager:v0.28.1@sha256:27c475db5fb156cab31d5c18a4251ac7ed567746a2483ff264516437a39b15ba', '--config.file=/rules/stack/alertmanager.yml', '--cluster.listen-address=')
  writeFileSync(join(directory, 'prometheus.json'), JSON.stringify({ global: { scrape_interval: '1s', evaluation_interval: '1s' }, rule_files: ['/rules/prometheus-rules.yaml'], alerting: { alertmanagers: [{ static_configs: [{ targets: ['alertmanager:9093'] }] }] }, scrape_configs: [{ job_name: 'wts-ha', static_configs: [{ targets: ['fixture:8080'], labels: { namespace: 'wts-ha', pod: 'gateway' } }] }] }))
  start('prometheus', 9090, prom, '--config.file=/config/prometheus.json')
  const lokiConfig = readFileSync(join(source, 'stack/loki-config.yaml'), 'utf8').replaceAll('/var/loki', '/tmp/loki').replace('directory: /etc/loki/rules', 'directory: /config/rules')
  const { mkdirSync } = await import('node:fs')
  mkdirSync(join(directory, 'rules/fake'), { recursive: true })
  writeFileSync(join(directory, 'rules/fake/rules.yaml'), readFileSync(join(source, 'loki-rules.yaml')))
  writeFileSync(join(directory, 'loki.yaml'), lokiConfig)
  const loki = start('loki', 3100, 'grafana/loki:3.5.3@sha256:3165cecce301ce5b9b6e3530284b080934a05cd5cafac3d3d82edcb887b45ecd', '-config.file=/config/loki.yaml')
  const events = async () => (await (await fetch(`${receiver}/notifications`)).json()).flatMap(event => event.alerts)
  const received = async (name, status) => (await events()).some(event => event.name === name && event.status === status && event.namespace === 'wts-ha')
  await wait(async () => (await fetch(`${loki}/ready`)).ok, 'Loki startup', 120000)
  await wait(async () => (await fetch(`${fixture}/metrics`)).ok, 'fixture startup')
  await fetch(`${fixture}/fault`)
  await wait(() => received('WtsHaSourceStale', 'firing'), 'Prometheus firing notification')
  await sleep(12000)
  assert.equal((await events()).filter(event => event.name === 'WtsHaSourceStale' && event.status === 'firing').length, 1)
  await fetch(`${fixture}/recover`)
  await wait(() => received('WtsHaSourceStale', 'resolved'), 'Prometheus recovery notification')
  docker('stop', `${network}-fixture`)
  await wait(() => received('WtsHaMetricsMissing', 'firing'), 'missing endpoint notification')
  docker('start', `${network}-fixture`)
  await wait(() => received('WtsHaMetricsMissing', 'resolved'), 'scrape recovery notification')
  const push = async namespace => {
    const response = await fetch(`${loki}/loki/api/v1/push`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ streams: [{ stream: { namespace, component: 'feed' }, values: [[String(BigInt(Date.now()) * 1000000n), JSON.stringify({ fields: { event: 'ha_producer_stopped', reason: 'producer-fenced' } })]] }] }) })
    assert.equal(response.status, 204)
  }
  await push('wts-ha'); await push('other')
  await sleep(12000)
  assert.equal(await received('WtsHaUnexpectedFencing', 'firing'), false)
  await push('wts-ha')
  await wait(() => received('WtsHaUnexpectedFencing', 'firing'), 'Loki repeated fencing notification')
  assert.equal((await events()).some(event => event.name === 'WtsHaUnexpectedFencing' && event.namespace === 'other'), false)
  await wait(() => received('WtsHaUnexpectedFencing', 'resolved'), 'Loki recovery notification', 330000)
  console.log('Prometheus/Loki 발생, 중복 억제, namespace 분리, endpoint 소실 및 복구 통지 통과')
} finally {
  for (const name of names.reverse()) { try { docker('rm', '-f', name) } catch {} }
  try { docker('network', 'rm', network) } catch {}
  rmSync(directory, { recursive: true, force: true })
}

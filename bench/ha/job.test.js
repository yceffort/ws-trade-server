import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { loadJob } from './job.js'
import { render } from '../../deploy/k8s/ha/render.js'
const settings = {
  ...JSON.parse(readFileSync(new URL('../../deploy/k8s/ha/settings.example.json', import.meta.url))),
  loadImage: `example/load@sha256:${'1'.repeat(64)}`
}
test('ordinary load has no mutation role or mounted API token', () => {
  const items = loadJob(settings).items
  assert.ok(!items.some(o => o.kind === 'Role'))
  const pod = items.find(o => o.kind === 'Job').spec.template.spec
  assert.equal(pod.automountServiceAccountToken, false)
  assert.ok(!pod.volumes.some(v => v.name === 'chaos'))
  assert.throws(() => loadJob(settings, { actions: [{ name: 'fault' }] }), /explicit enableFaults/)
  assert.throws(() => loadJob({ ...settings, namespace: 'production' }), /dedicated namespace/)
})
test('opted in faults are namespace bound and broker mutations target one StatefulSet', () => {
  const items = loadJob(settings, { enableFaults: true }).items
  const role = items.find(o => o.kind === 'Role')
  assert.deepEqual(role.rules.find(r => r.resources.includes('statefulsets')).resourceNames, ['wts-broker'])
  assert.ok(items.every(o => o.metadata.namespace === settings.namespace))
})
test('metrics expose only a separate internal service and monitoring namespace ingress', () => {
  const items = render({ ...settings, metricsNamespace: 'monitoring' }).items
  const service = items.find(o => o.kind === 'Service' && o.metadata.name === 'wts-ha-metrics')
  assert.equal(service.spec.type, undefined)
  assert.equal(service.spec.ports[0].port, 9090)
  const policy = items.find(o => o.kind === 'NetworkPolicy' && o.metadata.name === 'ha-metrics')
  assert.equal(
    policy.spec.ingress[0].from[0].namespaceSelector.matchLabels['kubernetes.io/metadata.name'],
    'monitoring'
  )
  assert.deepEqual(
    items.find(o => o.kind === 'Service' && o.metadata.name === 'wts-gateway').spec.ports.map(p => p.port),
    [443]
  )
})

test('Job and collection budgets cover the requested measurement duration', () => {
  const job = durationSeconds => loadJob(settings, { durationSeconds }).items.find(o => o.kind === 'Job')
  assert.equal(job(600).spec.activeDeadlineSeconds, 1200)
  assert.equal(job(900).spec.activeDeadlineSeconds, 1500)
  for (const value of [9, 901, 10.5, '600', null]) assert.throws(() => job(value), /durationSeconds/)
})

test('external LB egress is explicit and validates CIDR families and TCP ports', () => {
  const urls = ['wss://lb.example/experimental/ha/ws']
  assert.throws(() => loadJob(settings, { urls }), /externalEgress/)
  const externalEgress = [
    { cidr: '198.51.100.10/32', port: 443 },
    { cidr: '2001:db8::/64', port: 8443 }
  ]
  const policy = loadJob(settings, { urls, externalEgress }).items.find(
    o => o.kind === 'NetworkPolicy' && o.metadata.name === 'wts-ha-load'
  )
  assert.deepEqual(
    policy.spec.egress.slice(3),
    externalEgress.map(r => ({ to: [{ ipBlock: { cidr: r.cidr } }], ports: [{ protocol: 'TCP', port: r.port }] }))
  )
  for (const cidr of ['999.1.1.1/24', '10.0.0.0/33', '::/129', '10.0.0.1/-1', '10.0.0.1'])
    assert.throws(() => loadJob(settings, { externalEgress: [{ cidr, port: 443 }] }), /externalEgress/)
  for (const port of [0, 65536, '443', 1.5])
    assert.throws(() => loadJob(settings, { externalEgress: [{ cidr: '10.0.0.0/8', port }] }), /externalEgress/)
})

test('load Pod can be pinned to dedicated tainted nodes without leaking placement into the load config', () => {
  const nodeSelector = { 'wts.example/role': 'load' },
    tolerations = [{ key: 'dedicated', operator: 'Equal', value: 'load', effect: 'NoSchedule' }]
  const items = loadJob(settings, { nodeSelector, tolerations }).items
  const pod = items.find(o => o.kind === 'Job').spec.template.spec
  assert.deepEqual(pod.nodeSelector, nodeSelector)
  assert.deepEqual(pod.tolerations, tolerations)
  const config = JSON.parse(items.find(o => o.kind === 'ConfigMap').data['load.json'])
  assert.equal(config.nodeSelector, undefined)
  assert.equal(config.tolerations, undefined)
  const plain = loadJob(settings).items.find(o => o.kind === 'Job').spec.template.spec
  assert.equal(plain.nodeSelector, undefined)
  assert.equal(plain.tolerations, undefined)
  for (const value of ['load', [], { role: 1 }])
    assert.throws(() => loadJob(settings, { nodeSelector: value }), /nodeSelector/)
  for (const value of [{}, [{ key: 1, effect: 'NoSchedule' }], [{ key: 'dedicated', effect: 'Sometimes' }]])
    assert.throws(() => loadJob(settings, { tolerations: value }), /tolerations/)
})

test('tolerations follow the Kubernetes rules for omitted key, value and effect', () => {
  const pod = tolerations => loadJob(settings, { tolerations }).items.find(o => o.kind === 'Job').spec.template.spec
  for (const value of [
    [{ key: 'dedicated', operator: 'Exists' }],
    [{ operator: 'Exists', effect: 'NoSchedule' }],
    [{ operator: 'Exists' }],
    [{ key: 'dedicated', value: 'load' }],
    [{ key: 'dedicated', operator: '', value: 'load' }],
    [{ key: 'dedicated', effect: '' }],
    [{ key: 'dedicated', operator: 'Equal', value: 'load', effect: 'NoSchedule' }]
  ])
    assert.deepEqual(pod(value).tolerations, value)
  for (const value of [
    [{ effect: 'NoSchedule' }],
    [{ operator: 'Equal', value: 'load' }],
    [{ key: '', value: 'load' }],
    [{ key: 'dedicated', operator: 'Exists', value: 'load' }],
    [{ key: 'dedicated', operator: 'In' }],
    [null],
    ['dedicated']
  ])
    assert.throws(() => loadJob(settings, { tolerations: value }), /tolerations/)
})

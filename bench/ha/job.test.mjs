import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { loadJob } from './job.mjs'
import { render } from '../../deploy/k8s/ha/render.mjs'
const settings={...JSON.parse(readFileSync(new URL('../../deploy/k8s/ha/settings.example.json',import.meta.url))),loadImage:`example/load@sha256:${'1'.repeat(64)}`}
test('ordinary load has no mutation role or mounted API token',()=>{
  const items=loadJob(settings).items
  assert.ok(!items.some(o=>o.kind==='Role'))
  const pod=items.find(o=>o.kind==='Job').spec.template.spec
  assert.equal(pod.automountServiceAccountToken,false)
  assert.ok(!pod.volumes.some(v=>v.name==='chaos'))
  assert.throws(()=>loadJob(settings,{actions:[{name:'fault'}]}),/explicit enableFaults/)
  assert.throws(()=>loadJob({...settings,namespace:'production'}),/dedicated namespace/)
})
test('opted in faults are namespace bound and broker mutations target one StatefulSet',()=>{
  const items=loadJob(settings,{enableFaults:true}).items
  const role=items.find(o=>o.kind==='Role')
  assert.deepEqual(role.rules.find(r=>r.resources.includes('statefulsets')).resourceNames,['wts-broker'])
  assert.ok(items.every(o=>o.metadata.namespace===settings.namespace))
})
test('metrics expose only a separate internal service and monitoring namespace ingress',()=>{
  const items=render({...settings,metricsNamespace:'monitoring'}).items
  const service=items.find(o=>o.kind==='Service'&&o.metadata.name==='wts-ha-metrics')
  assert.equal(service.spec.type,undefined)
  assert.equal(service.spec.ports[0].port,9090)
  const policy=items.find(o=>o.kind==='NetworkPolicy'&&o.metadata.name==='ha-metrics')
  assert.equal(policy.spec.ingress[0].from[0].namespaceSelector.matchLabels['kubernetes.io/metadata.name'],'monitoring')
  assert.deepEqual(items.find(o=>o.kind==='Service'&&o.metadata.name==='wts-gateway').spec.ports.map(p=>p.port),[443])
})

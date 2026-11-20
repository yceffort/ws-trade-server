import test from 'node:test'
import assert from 'node:assert/strict'
import { drive } from './rollout.mjs'
import { render } from './render.mjs'
import { readFileSync } from 'node:fs'
const settings=JSON.parse(readFileSync(new URL('./settings.example.json',import.meta.url)))
const plan={version:'v2',gatewayImage:`example/gateway@sha256:${'1'.repeat(64)}`,edgeImage:`example/edge@sha256:${'2'.repeat(64)}`}
function fixture() {
  const deployments=new Map(render(settings).items.filter(o=>o.kind==='Deployment'&&o.metadata.name.startsWith('wts-gateway')).map(o=>[o.metadata.name,o]))
  for(const d of deployments.values()) d.metadata.resourceVersion='1'
  const api={state:null,owner:null,changes:[],settledSlots:[],async lock(owner){if(this.owner)throw new Error('locked');this.owner=owner},async unlock(owner){assert.equal(this.owner,owner);this.owner=null},async read(){return structuredClone(this.state)},async save(state){this.state=structuredClone(state)},async readyAll(){return [{uid:'a'},{uid:'b'},{uid:'c'}]},async deployment(name){return structuredClone(deployments.get(name))},async patch(name,version,template){const d=deployments.get(name);assert.equal(d.metadata.resourceVersion,version);d.spec.template=structuredClone(template);d.metadata.resourceVersion=String(Number(version)+1);this.changes.push(name)},async settled(name){this.settledSlots.push(name)}}
  return api
}
const accepted=request=>({...request,ok:true,committed_missing:0,duplicate_applies:0})
test('next slot remains unchanged until reconnect verification succeeds, then resume skips verified slots',async()=>{
  const api=fixture()
  let count=0
  await assert.rejects(drive(api,plan,async request=>{if(++count===2)throw new Error('oracle unavailable');return accepted(request)}),/oracle unavailable/)
  assert.deepEqual(api.changes,['wts-gateway-a','wts-gateway-b'])
  assert.equal(api.state.completed,1)
  assert.equal(api.owner,null)
  const result=await drive(api,plan,async request=>accepted(request))
  assert.equal(result.completed,3)
  assert.deepEqual(api.changes,['wts-gateway-a','wts-gateway-b','wts-gateway-c'])
})
test('interruption after patch resumes same slot without patching or skipping validation',async()=>{
  const api=fixture()
  const save=api.save.bind(api)
  let crashed=false
  api.save=async state=>{if(state.steps[0].phase==='patched'&&!crashed){crashed=true;throw new Error('interrupted')}await save(state)}
  await assert.rejects(drive(api,plan,accepted),/interrupted/)
  assert.equal(api.state.completed,0)
  assert.equal(api.state.steps[0].phase,'prepared')
  await drive(api,plan,accepted)
  assert.deepEqual(api.changes,['wts-gateway-a','wts-gateway-b','wts-gateway-c'])
})
test('failed replay validation, concurrent drivers, and changed templates fail closed',async()=>{
  const api=fixture()
  await assert.rejects(drive(api,plan,request=>({...accepted(request),committed_missing:1})),/validation failed/)
  assert.deepEqual(api.changes,['wts-gateway-a'])
  api.owner='other'
  await assert.rejects(drive(api,plan,accepted),/locked/)
  assert.equal(api.owner,'other')
  api.owner=null
  const original=api.deployment.bind(api)
  api.deployment=async name=>{const d=await original(name);d.spec.template.spec.containers[0].image='unexpected';return d}
  await assert.rejects(drive(api,plan,accepted),/unexpected template/)
  assert.deepEqual(api.changes,['wts-gateway-a'])
})
test('rollback restores saved full templates in reverse slot order and verifies each',async()=>{
  const api=fixture()
  const before=(await api.deployment('wts-gateway-a')).spec.template
  await drive(api,plan,accepted)
  const result=await drive(api,plan,accepted,{rollback:true})
  assert.equal(result.rollback,true)
  assert.deepEqual(api.changes.slice(3),['wts-gateway-c','wts-gateway-b','wts-gateway-a'])
  assert.deepEqual((await api.deployment('wts-gateway-a')).spec.template,before)
})
test('manifest preserves three zones, single surge, loopback probes, independent quorum and minimal lease RBAC',()=>{
  const items=render(settings).items
  const gateways=items.filter(o=>o.kind==='Deployment'&&o.metadata.name.startsWith('wts-gateway'))
  assert.equal(gateways.length,3)
  assert.equal(new Set(gateways.map(d=>d.spec.template.spec.nodeSelector['topology.kubernetes.io/zone'])).size,3)
  for(const d of gateways){
    assert.deepEqual(d.spec.strategy.rollingUpdate,{maxUnavailable:0,maxSurge:1})
    assert.equal(d.spec.template.spec.automountServiceAccountToken,false)
    assert.equal(d.spec.template.spec.initContainers[0].restartPolicy,'Always')
    assert.deepEqual(d.spec.template.spec.containers[0].readinessProbe.exec.command,['container-probe','ha-ready'])
    assert.equal(d.spec.template.spec.terminationGracePeriodSeconds,60)
    assert.equal(d.spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution[0].topologyKey,'kubernetes.io/hostname')
  }
  const broker=items.find(o=>o.kind==='StatefulSet')
  assert.equal(broker.spec.replicas,3)
  assert.equal(broker.spec.updateStrategy.type,'OnDelete')
  assert.equal(broker.spec.persistentVolumeClaimRetentionPolicy.whenDeleted,'Retain')
  const role=items.find(o=>o.kind==='Role'&&o.metadata.name==='wts-feed')
  assert.deepEqual(role.rules[0].verbs,['get','update'])
  assert.deepEqual(role.rules[0].resourceNames,['wts-feed'])
  assert.ok(items.find(o=>o.kind==='NetworkPolicy'&&o.metadata.name==='default-deny'))
  assert.throws(()=>render({...settings,zones:['same','same','same']}),/distinct zones/)
  assert.throws(()=>render({...settings,gatewayImage:'example:latest'}),/digests required/)
})

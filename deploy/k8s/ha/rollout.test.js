import test from 'node:test'
import assert from 'node:assert/strict'
import { drive, Kubernetes, stablePod, kubectlFailure } from './rollout.js'
import { render } from './render.js'
import { readFileSync } from 'node:fs'
const settings=JSON.parse(readFileSync(new URL('./settings.example.json',import.meta.url)))
const plan={version:'v2',gatewayImage:`example/gateway@sha256:${'1'.repeat(64)}`,edgeImage:`example/edge@sha256:${'2'.repeat(64)}`}
function fixture() {
  const deployments=new Map(render(settings).items.filter(o=>o.kind==='Deployment'&&o.metadata.name.startsWith('wts-gateway')).map(o=>[o.metadata.name,o]))
  for(const d of deployments.values()) d.metadata.resourceVersion='1'
  const api={state:null,owner:null,changes:[],settledSlots:[],async lock(owner){if(this.owner)throw new Error('locked');this.owner=owner},async unlock(owner){assert.equal(this.owner,owner);this.owner=null},async read(){return structuredClone(this.state)},async save(state){this.state=structuredClone(state)},async readyOthers(){return []},async readyAll(){return [{uid:'a'},{uid:'b'},{uid:'c'}]},async deployment(name){return structuredClone(deployments.get(name))},async patch(name,version,template){const d=deployments.get(name);assert.equal(d.metadata.resourceVersion,version);d.spec.template=structuredClone(template);d.metadata.resourceVersion=String(Number(version)+1);this.changes.push(name)},async settled(name){this.settledSlots.push(name)}}
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
test('resume stops before another patch if an already verified slot drifted',async()=>{
  const api=fixture()
  await assert.rejects(drive(api,plan,async request=>{
    if(request.slot==='wts-gateway-b')throw new Error('oracle unavailable')
    return accepted(request)
  }),/oracle unavailable/)
  const current=await api.deployment('wts-gateway-a')
  await api.patch('wts-gateway-a',current.metadata.resourceVersion,api.state.steps[0].before)
  const changes=[...api.changes]
  await assert.rejects(drive(api,plan,accepted),/completed slot template changed/)
  assert.deepEqual(api.changes,changes)
  assert.equal(api.state.completed,1)
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
  // 보관 설정은 새 토픽 생성과 기존 토픽 변경에 같은 값으로 들어간다.
  const bootstrap=JSON.stringify(render({...settings,retentionBytes:8589934592}).items.find(o=>o.kind==='Job'&&o.metadata.name.startsWith('wts-bootstrap')))
  assert.match(bootstrap,/--config retention\.bytes=8589934592/)
  assert.match(bootstrap,/--alter --entity-type topics --entity-name wts\.events\.v1 --add-config retention\.ms=86400000,retention\.bytes=8589934592/)
  for(const retention of [{retentionBytes:1024},{retentionMs:'86400000'},{retentionBytes:-1}])assert.throws(()=>render({...settings,...retention}),/invalid event retention/)
  const edgeOf=extra=>render({...settings,...extra}).items.find(o=>o.kind==='Deployment'&&o.metadata.name.startsWith('wts-gateway')).spec.template.spec.initContainers.find(c=>c.name==='edge')
  assert.equal(edgeOf({}).env.find(e=>e.name==='WTS_EDGE_WORKERS').value,'2')
  assert.equal(edgeOf({}).resources.limits.memory,'1Gi')
  assert.equal(edgeOf({edgeWorkers:1}).resources.limits.memory,'512Mi')
  assert.throws(()=>render({...settings,edgeWorkers:3}),/edgeWorkers/)
})

test('rollback repairs failed second slot before checking untouched third slot',async()=>{
  const api=fixture(),patch=api.patch.bind(api)
  let failed=null
  api.readyAll=async()=>{if(failed)throw new Error('gateway unready');return []}
  api.readyOthers=async name=>{assert.equal(name===failed || failed===null,true)}
  api.patch=async(name,version,template)=>{
    await patch(name,version,template)
    if(name==='wts-gateway-b') failed=template.metadata.annotations['wts/config-version']==='v2'?name:null
  }
  api.settled=async()=>{if(failed)throw new Error('slot stabilization deadline exceeded')}
  await assert.rejects(drive(api,plan,accepted),/deadline exceeded/)
  assert.equal(api.state.completed,1)
  const result=await drive(api,plan,accepted,{rollback:true})
  assert.equal(result.completed,3)
  assert.deepEqual(api.changes,['wts-gateway-a','wts-gateway-b','wts-gateway-b','wts-gateway-a'])
})
test('rollback does not change a failed slot when another slot is also unavailable',async()=>{
  const api=fixture()
  await assert.rejects(drive(api,plan,()=>{throw new Error('failed')}),/failed/)
  api.readyOthers=async()=>{throw new Error('another slot unavailable')}
  await assert.rejects(drive(api,plan,accepted,{rollback:true}),/another slot unavailable/)
  assert.deepEqual(api.changes,['wts-gateway-a'])
})
test('existing corrupt journal is rejected before deployment mutation',async()=>{
  for (const value of [null,false,0,{}, {steps:[]}, {completed:4}]) {
    const api=new Kubernetes('test','wts-ha')
    let changes=0
    api.get=async()=>({metadata:{resourceVersion:'1'},data:{journal:JSON.stringify(value)}})
    api.lock=api.unlock=async()=>{}
    api.patch=async()=>{changes++}
    await assert.rejects(drive(api,plan,accepted),/invalid rollout journal/)
    assert.equal(changes,0)
  }
  const api=fixture()
  await drive(api,plan,accepted)
  api.state.steps[1].name=api.state.steps[0].name
  await assert.rejects(drive(api,plan,accepted),/invalid rollout journal/)
})
test('ready age must be continuous and at least fifteen seconds',()=>{
  const now=Date.now(),pod={metadata:{},status:{conditions:[{type:'Ready',status:'True',lastTransitionTime:new Date(now-15000).toISOString()}]}}
  assert.equal(stablePod(pod,now),true)
  pod.status.conditions[0].lastTransitionTime=new Date(now-14999).toISOString()
  assert.equal(stablePod(pod,now),false)
  pod.status.conditions[0].lastTransitionTime='invalid'
  assert.equal(stablePod(pod,now),false)
})
test('manifest name limit and post-SNAT network range are separate from LB clients',()=>{
  assert.throws(()=>render({...settings,version:'v'.repeat(50)}),/version/)
  for(const item of render({...settings,version:'v'.repeat(49)}).items) assert.ok(item.metadata.name.length<=63)
  const items=render({...settings,clientCIDRs:['198.51.100.0/24'],gatewayIngressCIDRs:['10.0.0.0/8']}).items
  assert.deepEqual(items.find(o=>o.kind==='Service'&&o.metadata.name==='wts-gateway').spec.loadBalancerSourceRanges,['198.51.100.0/24'])
  assert.deepEqual(items.find(o=>o.kind==='NetworkPolicy'&&o.metadata.name==='gateway').spec.ingress[0].from,[{ipBlock:{cidr:'10.0.0.0/8'}}])
})
test('gateway Service annotations and extra certificate names come from settings',()=>{
  const service=s=>render(s).items.find(o=>o.kind==='Service'&&o.metadata.name==='wts-gateway')
  const annotations={'service.beta.kubernetes.io/aws-load-balancer-type':'external','service.beta.kubernetes.io/aws-load-balancer-nlb-target-type':'ip'}
  assert.deepEqual(service({...settings,gatewayServiceAnnotations:annotations}).metadata.annotations,annotations)
  assert.equal(service(settings).metadata.annotations,undefined)
  for(const value of [[],'external',{'bad key':'v'},{'example.com/a':1}])assert.throws(()=>render({...settings,gatewayServiceAnnotations:value}),/Service annotations/)
  render({...settings,gatewayNames:['gateway.wts-ha.internal']})
  for(const value of ['gateway.internal',['*.example.com'],['a,DNS:evil'],['UPPER.example']])assert.throws(()=>render({...settings,gatewayNames:value}),/gateway names/)
})

test('terminated Pod history does not block rollout, while deleting Pods still do',async()=>{
  const api=new Kubernetes('test','wts-ha')
  const pod=(name,phase,deletionTimestamp)=>({metadata:{name,deletionTimestamp},status:{phase}})
  api.command=async()=>JSON.stringify({items:[pod('evicted','Failed'),pod('finished','Succeeded'),pod('current','Running'),pod('old','Running','now'),pod('deleting-failed','Failed','now')]})
  assert.deepEqual((await api.pods()).map(p=>p.metadata.name),['current','old','deleting-failed'])
})
test('kubectl diagnostics distinguish lock conflicts without echoing credential text',()=>{
  for(const reason of ['AlreadyExists','Forbidden','Unauthorized','Conflict']){
    const message=kubectlFailure({},`Error from server (${reason}): synthetic-credential`).message
    assert.match(message,new RegExp(reason));assert.ok(!message.includes('synthetic-credential'))
  }
  assert.match(kubectlFailure({killed:true},'').message,/Deadline/)
})

test('measurement acknowledgement precedes every patch and windows survive retry',async()=>{
  const api=fixture(),begun=new Map(),patch=api.patch.bind(api)
  const begin=async request=>{begun.set(request.slot,request.window)}
  api.patch=async(...args)=>{assert.ok(begun.has(args[0]));return patch(...args)}
  await assert.rejects(drive(api,plan,()=>{throw new Error('oracle unavailable')},{begin}),/oracle unavailable/)
  const first=begun.get('wts-gateway-a')
  await drive(api,plan,request=>{assert.equal(request.window,begun.get(request.slot));return accepted(request)},{begin})
  assert.equal(begun.get('wts-gateway-a'),first)
  assert.equal(new Set(begun.values()).size,3)
  const untouched=fixture()
  await assert.rejects(drive(untouched,plan,accepted,{begin:()=>{throw new Error('measurement unavailable')}}),/measurement unavailable/)
  assert.deepEqual(untouched.changes,[])
})

test('failed stabilization cancels its measurement and releases the rollout lock',async()=>{
  const api=fixture(),cancelled=[]
  api.settled=async()=>{throw new Error('stabilization failed')}
  await assert.rejects(drive(api,plan,accepted,{cancel:request=>cancelled.push(request)}),/stabilization failed/)
  assert.equal(cancelled.length,1);assert.equal(cancelled[0].window,api.state.steps[0].window)
  assert.equal(cancelled[0].phase,'cancel');assert.equal(api.owner,null)
  await assert.rejects(drive(api,plan,accepted,{cancel:()=>{throw new Error('control unavailable')}}),/control unavailable/)
  assert.equal(api.owner,null)
})
test('rollback cancels an interrupted forward measurement before opening a new one',async()=>{
  const api=fixture(),cancelled=[]
  await assert.rejects(drive(api,plan,()=>{throw new Error('interrupted')}),/interrupted/)
  const abandoned=api.state.steps[0].window
  await drive(api,plan,accepted,{rollback:true,cancel:request=>cancelled.push(request),begin:request=>{assert.equal(cancelled[0].window,abandoned);assert.notEqual(request.window,abandoned)}})
  assert.equal(api.state.completed,3);assert.equal(api.owner,null)
})

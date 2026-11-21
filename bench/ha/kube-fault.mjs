import { request } from 'node:https'
import { readFileSync } from 'node:fs'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { performance } from 'node:perf_hooks'
import { pathToFileURL } from 'node:url'
const exec=promisify(execFile)
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms))
const [action,namespace]=process.argv.slice(2)
const allowed=new Set(['gateway-kill','feed-failover','broker-one','broker-quorum','gateway-restart','full-restart'])
export function requestBody(method,body) {
  const data=body===undefined?undefined:JSON.stringify(body)
  const headers={'content-type':method==='PATCH'?'application/merge-patch+json':'application/json'}
  if(data!==undefined)headers['content-length']=String(Buffer.byteLength(data))
  return {data,headers}
}
export function responseJson(res,resolve,reject) {
  let text=''
  res.on('data',chunk=>{text+=chunk;if(text.length>4*1024*1024)res.destroy(new Error('API output limit'))})
  res.on('error',reject)
  res.on('close',()=>{if(!res.complete)reject(new Error('Kubernetes response interrupted'))})
  res.on('end',()=>{
    if(res.statusCode<200||res.statusCode>=300)return reject(Object.assign(new Error('Kubernetes action rejected'),{status:res.statusCode}))
    try{resolve(JSON.parse(text))}catch{reject(new Error('invalid Kubernetes response'))}
  })
}
export function faultPods(pods,includeTerminating=false) {
  return pods.filter(p=>includeTerminating&&p.metadata.deletionTimestamp||!p.metadata.deletionTimestamp&&!['Failed','Succeeded'].includes(p.status?.phase))
}
export async function restoreReplicas(api,path,uid) {
  for(let attempt=0;attempt<5;attempt++){
    const current=await api('GET',path)
    if(current.metadata.uid!==uid)throw new Error('broker identity changed during fault')
    try{return await api('PATCH',path,{metadata:{resourceVersion:current.metadata.resourceVersion},spec:{replicas:3}})}
    catch(error){if(error.status!==409||attempt===4)throw error;await delay(100)}
  }
}
export function recoveredPods(pods,removed,count,now=Date.now()) {
  return pods.length===count&&pods.every(p=>{
    const ready=p.status?.conditions?.find(c=>c.type==='Ready'),since=Date.parse(ready?.lastTransitionTime)
    return !p.metadata.deletionTimestamp&&!removed.has(p.metadata.uid)&&p.status?.phase==='Running'&&ready?.status==='True'&&Number.isFinite(since)&&now-since>=15000
  })
}
async function api(method,path,body){
  const token=readFileSync('/var/run/chaos/token','utf8').trim()
  const {data,headers}=requestBody(method,body)
  return await new Promise((resolve,reject)=>{
    const req=request(`https://kubernetes.default.svc/api${path}`,{method,ca:readFileSync('/var/run/chaos/ca.crt'),headers:{authorization:`Bearer ${token}`,...headers},timeout:3000},res=>{
      responseJson(res,resolve,reject)
    })
    req.on('error',reject);req.on('timeout',()=>req.destroy(new Error('Kubernetes request deadline')));req.end(data)
  })
}
const list=async (component,includeTerminating=false)=>faultPods((await api('GET',`/v1/namespaces/${namespace}/pods?labelSelector=${encodeURIComponent(`app.kubernetes.io/part-of=wts-ha,app.kubernetes.io/component=${component}`)}`)).items,includeTerminating)
async function remove(pod){
  if(pod.metadata.namespace!==namespace||pod.metadata.labels['app.kubernetes.io/part-of']!=='wts-ha')throw new Error('unexpected fault target')
  return await api('DELETE',`/v1/namespaces/${namespace}/pods/${pod.metadata.name}`,{apiVersion:'v1',kind:'DeleteOptions',gracePeriodSeconds:0,preconditions:{uid:pod.metadata.uid,resourceVersion:pod.metadata.resourceVersion}})
}
async function head(){const{stdout}=await exec('/app/ha-oracle',['head','/credentials/gateway.json','/source/source.json'],{timeout:15000,maxBuffer:65536});return JSON.parse(stdout)}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)try {
  if(!allowed.has(action)||!/^wts-ha(?:-[a-z0-9-]+)?$/.test(namespace)||process.env.WTS_HA_ALLOW_FAULTS!=='dedicated-test-namespace')throw new Error('dedicated test namespace and fault opt-in required')
  const started=performance.now()
  const removed=new Map()
  const deleteTarget=async pod=>{
    const component=pod.metadata.labels['app.kubernetes.io/component']
    if(!removed.has(component))removed.set(component,new Set())
    removed.get(component).add(pod.metadata.uid)
    return remove(pod)
  }
  let details={}
  if(action==='feed-failover'){
    const leasePath=`s/coordination.k8s.io/v1/namespaces/${namespace}/leases/wts-feed`
    const before=await api('GET',leasePath)
    const feed=(await list('feed')).find(p=>before.spec.holderIdentity?.startsWith(p.metadata.uid))
    if(!feed)throw new Error('active feed not found')
    const deleted=Date.now()
    await deleteTarget(feed)
    const deadline=performance.now()+30000
    let current
    while(performance.now()<deadline){
      const lease=await api('GET',leasePath)
      if(lease.spec.holderIdentity&&lease.spec.holderIdentity!==before.spec.holderIdentity){
        current=await head()
        if(current.source_status_at_ms&&BigInt(current.source_status_at_ms)>BigInt(deleted))break
      }
      await delay(250)
    }
    if(!current?.source_status_at_ms||BigInt(current.source_status_at_ms)<=BigInt(deleted))throw new Error('feed did not resume committed status within 30 seconds')
    const failover=Number(BigInt(current.source_status_at_ms)-BigInt(deleted))
    details={feed_failover_ms:failover,old_pod:feed.metadata.name}
    if(failover>30000)throw new Error('feed failover target exceeded')
  }else if(action==='broker-quorum'){
    const path=`s/apps/v1/namespaces/${namespace}/statefulsets/wts-broker`
    const before=await api('GET',path)
    if(before.spec.replicas!==3)throw new Error('three broker replicas required')
    try {
      await api('PATCH',path,{metadata:{resourceVersion:before.metadata.resourceVersion},spec:{replicas:1}})
      const deadline=performance.now()+60000
      while((await list('broker',true)).length>1&&performance.now()<deadline)await delay(500)
      if((await list('broker',true)).length!==1)throw new Error('quorum fault not established')
      await delay(15000)
    } finally {await restoreReplicas(api,path,before.metadata.uid)}
    details={quorum_pause_ms:15000}
    removed.set('broker',new Set())
  }else if(action==='full-restart'){
    for(const component of ['gateway','feed','broker']){
      const pods=await list(component)
      if(!pods.length)throw new Error('no fault target')
      for(const pod of pods)await deleteTarget(pod)
    }
    details={restarted:['gateway','feed','broker'],persistent_volumes_retained:true}
  }else{
    const pods=await list(action==='broker-one'?'broker':'gateway')
    if(!pods.length)throw new Error('no fault target')
    const targets=action==='gateway-restart'?pods:[pods.sort((a,b)=>a.metadata.name.localeCompare(b.metadata.name))[0]]
    for(const pod of targets)await deleteTarget(pod)
    details={deleted:targets.map(p=>p.metadata.name)}
  }
  const deadline=performance.now()+90000
  for(const [component,old] of removed){
    const count=component==='feed'?2:3
    while(!recoveredPods(await list(component,true),old,count)){
      if(performance.now()>=deadline)throw new Error('fault replacement readiness deadline')
      await delay(500)
    }
  }
  console.log(JSON.stringify({ok:true,action,elapsed_ms:performance.now()-started,...details}))
}catch(error){console.error(error.code?'HA fault cluster or oracle operation failed':error.message);process.exitCode=1}

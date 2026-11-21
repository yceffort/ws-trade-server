import { request } from 'node:https'
import { readFileSync } from 'node:fs'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { performance } from 'node:perf_hooks'
const exec=promisify(execFile)
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms))
const [action,namespace]=process.argv.slice(2)
const allowed=new Set(['gateway-kill','feed-failover','broker-one','broker-quorum','gateway-restart','full-restart'])
async function api(method,path,body){
  const token=readFileSync('/var/run/chaos/token','utf8').trim()
  return await new Promise((resolve,reject)=>{
    const req=request(`https://kubernetes.default.svc/api${path}`,{method,ca:readFileSync('/var/run/chaos/ca.crt'),headers:{authorization:`Bearer ${token}`,'content-type':method==='PATCH'?'application/merge-patch+json':'application/json'},timeout:3000},res=>{
      let text=''
      res.on('data',chunk=>{text+=chunk;if(text.length>4*1024*1024)req.destroy(new Error('API output limit'))})
      res.on('end',()=>res.statusCode>=200&&res.statusCode<300?resolve(JSON.parse(text)):reject(new Error('Kubernetes action rejected')))
    })
    req.on('error',reject);req.on('timeout',()=>req.destroy(new Error('Kubernetes request deadline')));req.end(body?JSON.stringify(body):undefined)
  })
}
const list=async (component,includeTerminating=false)=>(await api('GET',`/v1/namespaces/${namespace}/pods?labelSelector=${encodeURIComponent(`app.kubernetes.io/part-of=wts-ha,app.kubernetes.io/component=${component}`)}`)).items.filter(p=>includeTerminating||!p.metadata.deletionTimestamp)
async function remove(pod){
  if(pod.metadata.namespace!==namespace||pod.metadata.labels['app.kubernetes.io/part-of']!=='wts-ha')throw new Error('unexpected fault target')
  return await api('DELETE',`/v1/namespaces/${namespace}/pods/${pod.metadata.name}`,{apiVersion:'v1',kind:'DeleteOptions',gracePeriodSeconds:0,preconditions:{uid:pod.metadata.uid,resourceVersion:pod.metadata.resourceVersion}})
}
async function head(){const{stdout}=await exec('/app/ha-oracle',['head','/credentials/gateway.json','/source/source.json'],{timeout:15000,maxBuffer:65536});return JSON.parse(stdout)}
try {
  if(!allowed.has(action)||!/^wts-ha(?:-[a-z0-9-]+)?$/.test(namespace)||process.env.WTS_HA_ALLOW_FAULTS!=='dedicated-test-namespace')throw new Error('dedicated test namespace and fault opt-in required')
  const started=performance.now()
  let details={}
  if(action==='feed-failover'){
    const leasePath=`s/coordination.k8s.io/v1/namespaces/${namespace}/leases/wts-feed`
    const before=await api('GET',leasePath)
    const feed=(await list('feed')).find(p=>before.spec.holderIdentity?.startsWith(p.metadata.uid))
    if(!feed)throw new Error('active feed not found')
    const previous=await head()
    await remove(feed)
    const deleted=Date.now()
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
    const failover=Number(BigInt(current.source_status_at_ms)-BigInt(previous.source_status_at_ms))
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
    } finally {const current=await api('GET',path);await api('PATCH',path,{metadata:{resourceVersion:current.metadata.resourceVersion},spec:{replicas:3}})}
    details={quorum_pause_ms:15000}
  }else if(action==='full-restart'){
    for(const component of ['gateway','feed','broker'])for(const pod of await list(component))await remove(pod)
    details={restarted:['gateway','feed','broker'],persistent_volumes_retained:true}
  }else{
    const pods=await list(action==='broker-one'?'broker':'gateway')
    if(!pods.length)throw new Error('no fault target')
    const targets=action==='gateway-restart'?pods:[pods.sort((a,b)=>a.metadata.name.localeCompare(b.metadata.name))[0]]
    for(const pod of targets)await remove(pod)
    details={deleted:targets.map(p=>p.metadata.name)}
  }
  console.log(JSON.stringify({ok:true,action,elapsed_ms:performance.now()-started,...details}))
}catch{console.error('HA fault operation failed; inspect the dedicated test namespace');process.exitCode=1}

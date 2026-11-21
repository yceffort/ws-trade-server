import { createHash, randomUUID } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { pathToFileURL } from 'node:url'

const exec = promisify(execFile)
const slots = ['a','b','c'].map(slot => `wts-gateway-${slot}`)
const label = 'app.kubernetes.io/part-of=wts-ha,app.kubernetes.io/component=gateway'
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex')
const sleep = ms => new Promise(resolve => setTimeout(resolve,ms))
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value)
const validPlan = plan => object(plan) && typeof plan.version === 'string' && plan.version.length <= 49 && /^[a-z0-9][-a-z0-9]{0,47}[a-z0-9]$|^[a-z0-9]$/.test(plan.version) && [plan.gatewayImage,plan.edgeImage].every(s=>typeof s==='string' && /^[\w./:-]+@sha256:[a-f0-9]{64}$/.test(s))
export function validateJournal(journal) {
  const template = t => object(t) && object(t.metadata) && object(t.spec) && Array.isArray(t.spec.containers) && Array.isArray(t.spec.initContainers) && Array.isArray(t.spec.volumes)
  if (!object(journal) || !validPlan(journal.plan) || typeof journal.rollback !== 'boolean' || journal.planHash !== hash({plan:journal.plan,rollback:journal.rollback}) || !Number.isInteger(journal.completed) || journal.completed < 0 || journal.completed > 3 || !Array.isArray(journal.steps) || journal.steps.length !== 3 || new Set(journal.steps.map(s=>s?.name)).size !== 3 || journal.steps.some((s,i)=>!object(s) || !slots.includes(s.name) || !template(s.before) || !template(s.target) || !['pending','prepared','patched','verified'].includes(s.phase) || (i<journal.completed && s.phase!=='verified'))) throw new Error('invalid rollout journal; manual reconciliation required')
  return journal
}
export function stablePod(pod,now=Date.now()) {
  const ready=pod.status?.conditions?.find(c=>c.type==='Ready')
  const since=Date.parse(ready?.lastTransitionTime)
  return !pod.metadata.deletionTimestamp && ready?.status==='True' && Number.isFinite(since) && now-since>=15000
}
export function target(template,plan) {
  const next = structuredClone(template)
  next.metadata.annotations = {...next.metadata.annotations,'wts/config-version':plan.version}
  next.spec.containers.find(c=>c.name==='gateway').image = plan.gatewayImage
  next.spec.initContainers.find(c=>c.name==='edge').image = plan.edgeImage
  const metrics=next.spec.initContainers.find(c=>c.name==='metrics')
  if(metrics)metrics.image=plan.edgeImage
  next.spec.volumes.find(v=>v.name==='credentials').secret.secretName = `wts-gateway-${plan.version}`
  next.spec.volumes.find(v=>v.name==='tls').secret.secretName = `wts-tls-${plan.version}`
  next.spec.volumes.find(v=>v.name==='source').configMap.name = `wts-source-${plan.version}`
  return next
}
export async function drive(api,plan,verify,{rollback=false,stop=()=>false}={}) {
  if (!validPlan(plan)) throw new Error('version (at most 49 characters) and pinned images required')
  const owner = randomUUID()
  await api.lock(owner)
  try {
    const planHash = hash({plan,rollback})
    let journal = await api.read()
    if (journal !== null) validateJournal(journal)
    if (journal && journal.planHash !== planHash) {
      if (rollback && !journal.rollback) {
        const changed=[],unchanged=[]
        for (const step of [...journal.steps].reverse()) {
          const current=hash((await api.deployment(step.name)).spec.template)
          if (current!==hash(step.before) && current!==hash(step.target)) throw new Error('unexpected template change; manual reconciliation required')
          const restored={name:step.name,before:step.target,target:step.before,phase:'pending'}
          ;(current===hash(step.before)?unchanged:changed).push(restored)
        }
        journal = {planHash,rollback:true,plan,completed:0,steps:[...changed,...unchanged]}
        await api.save(journal)
      } else if (journal.completed !== 3) throw new Error('unfinished rollout uses a different plan')
      else journal = null
    }
    if (!journal) {
      if (rollback) throw new Error('rollback requires a saved previous rollout')
      await api.readyAll()
      const steps = []
      for (const name of slots) {
        const deployment = await api.deployment(name)
        steps.push({name,before:deployment.spec.template,target:target(deployment.spec.template,plan),phase:'pending'})
      }
      journal = {planHash,rollback:false,plan,completed:0,steps}
      await api.save(journal)
    }
    for (const step of journal.steps.slice(0,journal.completed)) {
      const current = await api.deployment(step.name)
      if (hash(current.spec.template) !== hash(step.target)) throw new Error('completed slot template changed; manual reconciliation required')
    }
    for (let index=journal.completed; index<journal.steps.length; index++) {
      if (stop()) throw new Error('rollout interrupted before next slot')
      const step = journal.steps[index]
      const current = await api.deployment(step.name)
      if (hash(current.spec.template) !== hash(step.target)) {
        if (hash(current.spec.template) !== hash(step.before)) throw new Error('unexpected template change; manual reconciliation required')
        if (journal.rollback) await api.readyOthers(step.name)
        else await api.readyAll()
        step.phase = 'prepared'
        await api.save(journal)
        if (stop()) throw new Error('rollout interrupted before patch')
        await api.patch(step.name,current.metadata.resourceVersion,step.target)
      }
      step.phase = 'patched'
      await api.save(journal)
      await api.settled(step.name,step.target,stop)
      const pods = await api.readyAll()
      if (stop()) throw new Error('rollout interrupted before validation')
      const nonce = randomUUID()
      const report = await verify({planHash,slot:step.name,nonce,pods,rollback:journal.rollback})
      if (report?.ok !== true || report.nonce !== nonce || report.planHash !== planHash || report.slot !== step.name || report.committed_missing !== 0 || report.duplicate_applies !== 0) throw new Error('reconnect/replay validation failed; next slot remains unchanged')
      step.phase = 'verified'
      step.report = report
      journal.completed = index+1
      await api.save(journal)
    }
    return journal
  } finally { await api.unlock(owner) }
}

export function kubectlFailure(error,stderr) {
  const reason=/\((AlreadyExists|Forbidden|Unauthorized|Conflict|NotFound|Timeout|ServerTimeout)\)/.exec(String(stderr))?.[1]??(error.killed?'Deadline':error.code==='ENOENT'?'ExecutableMissing':/connection refused|i\/o timeout|no such host/.test(String(stderr))?'Network':'Unknown')
  return new Error(`kubectl operation failed (${reason}); inspect cluster state`)
}

export class Kubernetes {
  constructor(context,namespace) {
    if (!context || !/^[a-z0-9][-a-z0-9]{0,61}[a-z0-9]$|^[a-z0-9]$/.test(namespace)) throw new Error('explicit context and namespace required')
    this.args=['--context',context,'--namespace',namespace,'--request-timeout=15s']
    this.namespace=namespace
    this.stateVersion=null
  }
  async command(args,input) {
    return await new Promise((resolve,reject)=> {
      const child=execFile('kubectl',[...this.args,...args],{timeout:20000,maxBuffer:2*1024*1024},(error,stdout,stderr)=>error?reject(kubectlFailure(error,stderr)):resolve(stdout))
      if(input!==undefined) child.stdin.end(JSON.stringify(input))
    })
  }
  object(name,data) { return {apiVersion:'v1',kind:'ConfigMap',metadata:{name,namespace:this.namespace},data} }
  async get(kind,name) {
    const text=await this.command(['get',kind,name,'--ignore-not-found','-o','json'])
    return text.trim()?JSON.parse(text):null
  }
  async lock(owner) { await this.command(['create','-f','-'],this.object('wts-rollout-lock',{owner,created_at:new Date().toISOString()})) }
  async unlock(owner) {
    const lock=await this.get('configmap','wts-rollout-lock')
    if(lock?.data.owner!==owner) throw new Error('rollout lock ownership lost')
    await this.command(['delete','-f','-','--wait=false'],lock)
  }
  async read() {
    const state=await this.get('configmap','wts-rollout-state')
    this.stateVersion=state?.metadata.resourceVersion??null
    return state?validateJournal(JSON.parse(state.data.journal)):null
  }
  async save(journal) {
    const serialized=JSON.stringify(journal)
    if(Buffer.byteLength(serialized)>256*1024) throw new Error('journal budget exceeded')
    const state=this.object('wts-rollout-state',{journal:serialized})
    if(this.stateVersion) state.metadata.resourceVersion=this.stateVersion
    const saved=JSON.parse(await this.command([this.stateVersion?'replace':'create','-f','-','-o','json'],state))
    this.stateVersion=saved.metadata.resourceVersion
  }
  async deployment(name) {
    const deployment=await this.get('deployment',name)
    if(!deployment || deployment.spec.replicas!==1 || deployment.spec.strategy?.rollingUpdate?.maxUnavailable!==0 || deployment.spec.strategy?.rollingUpdate?.maxSurge!==1) throw new Error('invalid gateway slot strategy')
    return deployment
  }
  async patch(name,version,template) {
    await this.command(['patch','deployment',name,'--type=json','-p',JSON.stringify([{op:'test',path:'/metadata/resourceVersion',value:version},{op:'replace',path:'/spec/template',value:template}])])
  }
  async pods() { return JSON.parse(await this.command(['get','pods','-l',label,'-o','json'])).items.filter(p=>p.metadata.deletionTimestamp || !['Failed','Succeeded'].includes(p.status?.phase)) }
  async readyAll() { return this.readyOthers(null) }
  async readyOthers(excluded) {
    const expected=slots.filter(name=>name!==excluded)
    const pods=(await this.pods()).filter(p=>!excluded || p.metadata.labels['wts-slot']!==excluded.at(-1))
    if(pods.length!==expected.length || expected.some(name=>!pods.some(p=>p.metadata.labels['wts-slot']===name.at(-1))) || pods.some(p=>!stablePod(p))) throw new Error('other gateway slots must be continuously ready for 15 seconds with no terminating pod')
    for (const name of expected) {
      const d=await this.deployment(name),s=d.status
      if(s?.observedGeneration!==d.metadata.generation || s?.availableReplicas!==1 || s?.updatedReplicas!==1 || s?.replicas!==1) throw new Error('all slots must be stable before proceeding')
    }
    return pods.map(p=>({uid:p.metadata.uid,name:p.metadata.name,node:p.spec.nodeName}))
  }
  async settled(name,template,stop) {
    const deadline=Date.now()+600000
    while(Date.now()<deadline) {
      if(stop()) throw new Error('rollout interrupted during stabilization')
      const d=await this.deployment(name)
      if(hash(d.spec.template)!==hash(template)) throw new Error('template changed during rollout')
      const pods=(await this.pods()).filter(p=>p.metadata.labels['wts-slot']===name.at(-1))
      if(d.status?.observedGeneration===d.metadata.generation && d.status.availableReplicas===1 && d.status.updatedReplicas===1 && d.status.replicas===1 && pods.length===1 && stablePod(pods[0])) return
      await sleep(2000)
    }
    throw new Error('slot stabilization deadline exceeded')
  }
}

if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href) {
  let stopped=false
  process.once('SIGTERM',()=>{stopped=true})
  process.once('SIGINT',()=>{stopped=true})
  try {
    const [context,namespace,path,mode='forward']=process.argv.slice(2)
    if(!['forward','rollback'].includes(mode)) throw new Error('mode must be forward or rollback')
    const config=JSON.parse(readFileSync(path))
    if(!Array.isArray(config.verifyCommand) || !config.verifyCommand.length || config.verifyCommand.some(a=>typeof a!=='string')) throw new Error('verification argv required')
    const verify=async request=>{
      const {stdout}=await exec(config.verifyCommand[0],config.verifyCommand.slice(1),{timeout:600000,maxBuffer:64*1024,env:{...process.env,WTS_ROLLOUT_REQUEST:JSON.stringify(request)}})
      return JSON.parse(stdout)
    }
    const result=await drive(new Kubernetes(context,namespace),config.plan,verify,{rollback:mode==='rollback',stop:()=>stopped})
    console.log(JSON.stringify({ok:true,planHash:result.planHash,completed:result.completed,rollback:result.rollback}))
  } catch(error) { console.error(error.message); process.exitCode=1 }
}

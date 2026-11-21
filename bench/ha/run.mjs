import { Worker } from 'node:worker_threads'
import { createServer } from 'node:http'
import { mkdirSync,readFileSync,writeFileSync,existsSync,unlinkSync,chmodSync } from 'node:fs'
import { dirname,resolve } from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { randomUUID } from 'node:crypto'
import { performance } from 'node:perf_hooks'
import { compare,percentile } from './oracle.mjs'
const exec=promisify(execFile)
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms))
const workers=[],exits=[],requests=new Map(),ready=new Set(),failures=[],verifications=[]
let server,config,stopped=false
async function command(argv,extra=[],timeout=90000){
  if(!Array.isArray(argv)||!argv.length||argv.some(s=>typeof s!=='string'))throw new Error('command argv required')
  const {stdout}=await exec(argv[0],[...argv.slice(1),...extra],{timeout,maxBuffer:128*1024*1024})
  return JSON.parse(stdout)
}
async function sample(){
  const id=randomUUID()
  return await new Promise((resolve,reject)=>{
    const replies=[]
    const timer=setTimeout(()=>{requests.delete(id);reject(new Error('client worker sample deadline'))},5000)
    requests.set(id,reply=>{replies.push(reply);if(replies.length===workers.length){clearTimeout(timer);requests.delete(id);resolve({clients:replies.flatMap(r=>r.clients),samples:replies.flatMap(r=>r.samples),client_event_loop_p99_ms:Math.max(...replies.map(r=>r.lag_p99_ms))})}})
    workers.forEach(worker=>worker.postMessage({type:'sample',id}))
  })
}
async function verify(request={}) {
  const boundary=await command(config.headCommand)
  const end=BigInt(boundary.next)-1n
  const deadline=performance.now()+config.recoveryDeadlineMs
  let observed
  do {
    observed=await sample()
    if(observed.clients.length===config.clients&&observed.clients.every(c=>!c.failed&&c.offset!==null&&BigInt(c.offset)>=end))break
    await delay(100)
  }while(performance.now()<deadline)
  if(observed.clients.some(c=>c.failed||c.offset===null||BigInt(c.offset)<end))throw new Error('clients did not apply the committed verification boundary')
  const maximum=observed.clients.reduce((n,c)=>BigInt(c.offset)>n?BigInt(c.offset):n,0n)
  const oracle=await command(config.rangeCommand,[config.start,maximum.toString()])
  const comparison=compare(oracle,observed.clients)
  const reconnect=percentile(observed.samples.filter(s=>s.disconnect_ms!==null).map(s=>s.disconnect_ms),.99)
  const recovery=percentile(observed.samples.filter(s=>s.fault_ms!==null).map(s=>s.fault_ms),.99)
  const slo=reconnect!==null&&reconnect<=5000
  const result={...request,ok:comparison.ok&&(!config.requireReconnectSlo||slo),committed_missing:comparison.ok?0:null,duplicate_applies:comparison.ok?0:null,comparison,reconnect_samples:observed.samples.length,reconnect_p99_ms:reconnect,recovery_from_fault_p99_ms:recovery,client_event_loop_p99_ms:observed.client_event_loop_p99_ms,verified_through:end.toString()}
  return result
}
try {
  config=JSON.parse(readFileSync(process.argv[2]))
  config.clients??=1000;config.workers??=4;config.durationSeconds??=600;config.recoveryDeadlineMs??=5000;config.requireReconnectSlo??=true;config.startupIntervalMs??=10
  if(!Number.isInteger(config.clients)||config.clients<1||config.clients>10000||!Number.isInteger(config.workers)||config.workers<1||config.workers>16||config.durationSeconds<10||config.durationSeconds>900)throw new Error('invalid load limits')
  if(!config.stateDirectory||existsSync(config.stateDirectory))throw new Error('fresh durable state directory required')
  config.stateDirectory=resolve(config.stateDirectory);mkdirSync(config.stateDirectory,{recursive:true,mode:0o700});chmodSync(config.stateDirectory,0o700)
  config.tokenFile=resolve(config.tokenFile)
  const head=await command(config.headCommand);config.start=head.next;config.topics??=head.topics
  if(!Array.isArray(config.urls)||!config.urls.length)throw new Error('HA endpoints required')
  for(const value of config.urls){const url=new URL(value);if(url.protocol!=='wss:'&&!(url.protocol==='ws:'&&['127.0.0.1','localhost','[::1]'].includes(url.hostname))||url.pathname!=='/experimental/ha/ws'||url.username||url.password||url.search)throw new Error('invalid HA endpoint')}
  for(let index=0;index<Math.min(config.workers,config.clients);index++){
    const ids=Array.from({length:config.clients},(_,i)=>i).filter(i=>i%config.workers===index)
    const worker=new Worker(new URL('./worker.mjs',import.meta.url),{workerData:{ids,config,head}})
    workers.push(worker)
    exits.push(new Promise(resolve=>worker.once('exit',resolve)))
    worker.on('message',message=>{
      if(message.type==='ready')ready.add(message.client)
      if(message.type==='failed')failures.push(message)
      if(message.type==='sample')requests.get(message.id)?.(message)
    })
    worker.on('error',()=>failures.push({reason:'worker-error'}))
    worker.on('exit',code=>{if(code!==0&&!stopped)failures.push({reason:'worker-exit'})})
  }
  const startup=performance.now()+60000
  while(ready.size<config.clients&&!failures.length&&performance.now()<startup)await delay(100)
  if(ready.size!==config.clients||failures.length)throw new Error('initial client catchup failed')
  const started=performance.now(),actions=[]
  if(config.controlSocket){
    config.controlSocket=resolve(config.controlSocket)
    if(existsSync(config.controlSocket))throw new Error('control socket already exists')
    server=createServer(async(request,response)=>{
      try {
        if(request.method!=='POST'||request.url!=='/verify')throw new Error('unknown control operation')
        let text=''
        for await(const chunk of request){text+=chunk;if(text.length>16384)throw new Error('control input limit')}
        const result=await verify(JSON.parse(text))
        verifications.push({slot:result.slot,planHash:result.planHash,rollback:result.rollback,ok:result.ok,reconnect_p99_ms:result.reconnect_p99_ms,verified_through:result.verified_through})
        response.writeHead(result.ok?200:409,{'content-type':'application/json'}).end(JSON.stringify(result))
      }catch{response.writeHead(409).end('{"ok":false}')}
    })
    server.maxConnections=1
    server.requestTimeout=10000;server.headersTimeout=5000;server.keepAliveTimeout=1000
    await new Promise(resolve=>server.listen(config.controlSocket,resolve));chmodSync(config.controlSocket,0o600)
  }
  const signal=()=>{stopped=true}
  process.once('SIGTERM',signal);process.once('SIGINT',signal)
  for(const action of config.actions??[]){
    if(!action.name||!Number.isFinite(action.atSeconds)||action.atSeconds<0||action.atSeconds>=config.durationSeconds)throw new Error('invalid fault schedule')
  }
  for(const action of [...(config.actions??[])].sort((a,b)=>a.atSeconds-b.atSeconds)){
    while(!stopped&&!failures.length&&performance.now()-started<action.atSeconds*1000)await delay(100)
    if(stopped||failures.length)break
    workers.forEach(w=>w.postMessage({type:'mark',name:action.name}))
    const at=performance.now()
    const result=await command(action.command,[],180000)
    actions.push({name:action.name,elapsed_ms:performance.now()-at,result})
    if(result.ok!==true)throw new Error('fault action failed')
  }
  while(!stopped&&!failures.length&&performance.now()-started<config.durationSeconds*1000)await delay(100)
  const report=await verify()
  const elapsed=performance.now()-started
  report.ok&&=failures.length===0
  report.duration_ms=elapsed;report.clients=config.clients;report.actions=actions;report.rollout_verifications=verifications;report.failures=failures
  report.environment=config.environment??{kind:'unspecified'}
  report.measured_targets_met=report.ok&&config.clients>=1000&&elapsed>=600000&&(actions.length>=3||verifications.filter(v=>v.ok).length>=3)
  report.production_slo_proven=false
  report.external_evidence_review_required=true
  mkdirSync(dirname(config.output),{recursive:true})
  writeFileSync(config.output,JSON.stringify(report,null,2),{mode:0o600})
  console.log(JSON.stringify({ok:report.ok,report:config.output,production_slo_proven:report.production_slo_proven}))
  if(!report.ok)process.exitCode=1
}catch(error){
  const reason=error.code?'external-command-or-io-failed':error.message
  if(config?.output){mkdirSync(dirname(config.output),{recursive:true});writeFileSync(config.output,JSON.stringify({ok:false,reason,clients:config.clients,ready:ready.size,failures,production_slo_proven:false},null,2),{mode:0o600})}
  console.error(reason);process.exitCode=1
}
finally{
  stopped=true
  for(const worker of workers)worker.postMessage({type:'stop'})
  server?.close();server?.closeAllConnections()
  if(config?.controlSocket&&existsSync(config.controlSocket))unlinkSync(config.controlSocket)
  await Promise.all(workers.map(async(worker,index)=>{const timer=setTimeout(()=>worker.terminate(),5000);await exits[index];clearTimeout(timer)}))
}

import { parentPort,workerData } from 'node:worker_threads'
import { performance,monitorEventLoopDelay } from 'node:perf_hooks'
import { join } from 'node:path'
import { runClient } from '../../examples/ha-client.mjs'
import { readBounded } from '../../examples/ha-state.mjs'
import { Journal } from './journal.mjs'
import { extend,payloadHash,zero,percentile } from './oracle.mjs'
import { Measurement } from './measurement.mjs'
import { REASONS,watch } from './reasons.mjs'

const {ids,config,head}=workerData
const controller=new AbortController()
const journal=new Journal(join(config.stateDirectory,`worker-${ids[0]}.journal`))
const clients=[]
const samples=[]
const reasons={}
const lag=monitorEventLoopDelay({resolution:20});lag.enable()
let marking=null
parentPort.on('message',message=>{
  if(message.type==='stop')controller.abort()
  if(message.type==='mark'){
    marking=message.scope?{...message.scope,at:performance.now()}:null
    for(const client of clients)client.measurement.mark(marking)
    parentPort.postMessage({type:'marked',id:message.id})
  }
  if(message.type==='sample')parentPort.postMessage({type:'sample',id:message.id,clients:clients.map(client=>{
    const state=client.store.durable()
    return {id:client.id,topics:state.topics,log_id:state.log_id,offset:state.offset,count:state.count,digest:state.digest,payload_digest:client.payloadDigest,failed:client.failed,caught_up:client.caughtUp,attempts:client.attempts,disconnects:client.disconnects,last_reason:client.lastReason}
  }),samples,reasons,lag_p99_ms:lag.count?lag.percentile(99)/1e6:null,lag_max_ms:lag.max/1e6})
})
const tasks=ids.map(async id=>{
  await new Promise(resolve=>setTimeout(resolve,id*config.startupIntervalMs))
  const client={id,store:journal.store(id,config.topics),payloadDigest:zero,failed:null,disconnects:0,measurement:new Measurement(),caughtUp:false,attempts:0,lastReason:null}
  client.measurement.mark(marking)
  clients.push(client)
  let attemptAt=0,pending=new Map(),initial=true,failed=()=>null
  try {
    await runClient({urls:config.urls,token:()=>readBounded(config.tokenFile,16384).toString('utf8').trim(),store:client.store,startAt:{log_id:head.log_id,partition:0,offset:head.next},signal:controller.signal,
      onAttempt(socket){client.attempts++;client.caughtUp=false;attemptAt=performance.now();pending=new Map();failed=watch(socket)},
      onDisconnect(reason){
        if(reason==='connection-error')reason=failed()??reason
        client.measurement.disconnect(performance.now());client.disconnects++;client.caughtUp=false;client.lastReason=['source-stale','heartbeat-timeout','connection-closed','draining','replay-limit','retry-later','receive-deadline','client-overflow',...REASONS].includes(reason)?reason:'other';reasons[client.lastReason]=(reasons[client.lastReason]??0)+1;parentPort.postMessage({type:'not-ready',client:id})
      },
      onFrame(frame,socket){
        if(frame.type==='event'){
          const digest=payloadHash(frame.body)
          if(pending.has(frame.event_id)&&pending.get(frame.event_id)!==digest)throw new Error('same id with changed payload')
          pending.set(frame.event_id,digest)
        }
        if(frame.type==='caught-up'&&socket.readyState===1){
          const timing=client.measurement.caughtUp(performance.now())
          if(initial){initial=false}
          else if(samples.length<100000){
            samples.push({client:id,attempt_ms:performance.now()-attemptAt,...timing})
          }
          client.caughtUp=true;parentPort.postMessage({type:'ready',client:id})
        }
      },
      async onCommit(state,ids){
        await client.store.flush()
        for(const id of ids){const body=pending.get(id);if(!body)throw new Error('committed payload absent');client.payloadDigest=extend(client.payloadDigest,id,body)}
        pending.clear()
      }
    })
  } catch(error){
    const known=new Set(['invalid-position','invalid-event-id','source-changed','invalid-event','log-changed','event-order','client-overflow','invalid-checkpoint','same id with changed payload','committed payload absent','client checkpoint batch limit'])
    client.failed=error.message
    const io=new Set(['ENOSPC','EIO','EMFILE','ENFILE','EROFS','EACCES'])
    parentPort.postMessage({type:'failed',client:id,reason:client.failed,apply_reason:known.has(error.cause?.message)?error.cause.message:io.has(error.cause?.code)?error.cause.code:'unknown'})
  }
})
await Promise.all(tasks)
await journal.close()
lag.disable()
parentPort.postMessage({type:'stopped',attempt_p99_ms:percentile(samples.map(s=>s.attempt_ms),.99)})

parentPort.close()

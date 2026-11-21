import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,writeFileSync,readFileSync,existsSync,rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawn } from 'node:child_process'
import { request } from 'node:http'
import { once } from 'node:events'
import { WebSocketServer } from '../../edge/node_modules/ws/wrapper.mjs'
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms))
for(const overlap of [null,'rollout-first','fault-first'])test(`control windows and measurement ownership: ${overlap??'isolated reconnect'}`,{timeout:20000},async()=>{
  const directory=mkdtempSync(join(tmpdir(),'wts-control-'))
  const server=new WebSocketServer({host:'127.0.0.1',port:0,handleProtocols:()=>'wts.ha.v1'})
  await once(server,'listening')
  let child,connections=0
  const timers=new Set()
  server.on('connection',socket=>{
    connections++
    const timer=setInterval(()=>{if(socket.readyState===1)socket.send('{"type":"heartbeat"}')},200)
    timers.add(timer);socket.on('close',()=>{clearInterval(timer);timers.delete(timer)})
    socket.on('message',bytes=>{
      const frame=JSON.parse(bytes)
      if(frame.type==='connect')socket.send('{"type":"connected"}')
      if(frame.type==='resume'){
        socket.send(JSON.stringify({type:'event',event_id:'demo/trades/epoch/1',topic:'a',position:{log_id:'test',partition:0,offset:'1'},body:{price:'1'}}))
        socket.send('{"type":"checkpoint","offset":"1","cursor":"synthetic-cursor"}')
        socket.send('{"type":"caught-up","offset":"1"}')
      }
    })
  })
  try {
    const oracle=join(directory,'oracle.mjs'),counter=join(directory,'head-called'),outage=join(directory,'oracle-outage'),broken=join(directory,'oracle-broken')
    writeFileSync(oracle,`import {existsSync,writeFileSync,unlinkSync} from 'node:fs';const broken=${JSON.stringify(broken)};if(existsSync(broken)){unlinkSync(broken);console.log('invalid-json');process.exit(0)}const outage=${JSON.stringify(outage)};if(existsSync(outage)){unlinkSync(outage);process.exit(1)}const counter=${JSON.stringify(counter)};if(process.argv[2]==='head'){const next=existsSync(counter)?'2':'1';writeFileSync(counter,'1');console.log(JSON.stringify({log_id:'test',next,topics:['a']}))}else console.log(JSON.stringify({log_id:'test',start:'1',through:'1',events:[{id:'demo/trades/epoch/1',topic:'a',offset:'1',body:{price:'1'}}]}));`)
    writeFileSync(join(directory,'token'),'Bearer synthetic')
    const socketPath=join(directory,'control.sock'),actionFile=join(directory,'action.mjs'),actionStarted=join(directory,'action-started')
    writeFileSync(actionFile,`import {writeFileSync} from 'node:fs';writeFileSync(${JSON.stringify(actionStarted)},'1');setTimeout(()=>console.log('{\"ok\":true}'),2000)`)
    writeFileSync(join(directory,'config.json'),JSON.stringify({clients:1,workers:1,durationSeconds:10,requireReconnectSlo:!overlap,actions:overlap?[{name:'synthetic',atSeconds:overlap==='rollout-first'?1:0,command:[process.execPath,actionFile]}]:[],urls:[`ws://127.0.0.1:${server.address().port}/experimental/ha/ws`],stateDirectory:join(directory,'state'),controlSocket:socketPath,output:join(directory,'report.json'),tokenFile:join(directory,'token'),headCommand:[process.execPath,oracle,'head'],rangeCommand:[process.execPath,oracle,'range']}))
    child=spawn(process.execPath,[fileURLToPath(new URL('./run.mjs',import.meta.url)),join(directory,'config.json')],{stdio:'ignore'})
    const exited=once(child,'exit')
    const call=(path,body)=>new Promise((resolve,reject)=>{
      const data=JSON.stringify(body),req=request({socketPath,path,method:'POST',headers:{'content-length':Buffer.byteLength(data)},timeout:5000},res=>{let text='';res.on('data',chunk=>text+=chunk);res.on('end',()=>{try{resolve(JSON.parse(text))}catch(error){reject(error)}});res.on('error',reject)})
      req.on('error',reject);req.on('timeout',()=>req.destroy(new Error('test control deadline')));req.end(data)
    })
    for(let i=0;i<100&&!existsSync(socketPath);i++)await delay(20)
    assert.ok(existsSync(socketPath))
    const first={window:'a',planHash:'plan',slot:'wts-gateway-a',rollback:false,nonce:'first'}
    if(overlap==='fault-first'){
      for(let i=0;i<100&&!existsSync(actionStarted);i++)await delay(20)
      assert.ok(existsSync(actionStarted))
      const rejected=await call('/begin',first)
      assert.equal(rejected.ok,false);assert.match(rejected.reason,/another measurement/)
      child.kill('SIGTERM');await exited;return
    }
    assert.equal((await call('/begin',first)).ok,true)
    if(overlap==='rollout-first'){
      await exited
      assert.equal(existsSync(actionStarted),false)
      assert.match(JSON.parse(readFileSync(join(directory,'report.json'))).reason,/overlaps rollout/)
      return
    }
    assert.equal((await call('/begin',{...first,window:'conflicting'})).ok,false)
    for(const socket of server.clients)socket.terminate()
    for(let i=0;i<100&&connections<2;i++)await delay(20)
    writeFileSync(outage,'temporary broker interruption')
    const verified=await call('/verify',first)
    assert.equal(verified.ok,true);assert.equal(verified.reconnect_samples,1)
    const second={...first,window:'b',slot:'wts-gateway-b',nonce:'second'}
    assert.equal((await call('/verify',second)).ok,false)
    assert.equal((await call('/begin',second)).ok,true)
    const empty=await call('/verify',second)
    assert.equal(empty.ok,false);assert.equal(empty.reconnect_slo_status,'not-measured')
    const third={...first,window:'third'}
    assert.equal((await call('/begin',third)).ok,true)
    writeFileSync(broken,'1')
    assert.equal((await call('/verify',third)).ok,false)
    assert.equal((await call('/begin',{...third,window:'rollback',rollback:true})).ok,true)
    child.kill('SIGTERM');await exited
  } finally {
    if(child?.exitCode===null){child.kill('SIGKILL');await once(child,'exit')}
    for(const timer of timers)clearInterval(timer)
    for(const socket of server.clients)socket.terminate()
    await new Promise(resolve=>server.close(resolve));rmSync(directory,{recursive:true,force:true})
  }
})

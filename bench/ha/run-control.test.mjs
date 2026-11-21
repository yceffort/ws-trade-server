import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,writeFileSync,existsSync,rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawn } from 'node:child_process'
import { request } from 'node:http'
import { once } from 'node:events'
import { WebSocketServer } from '../../edge/node_modules/ws/wrapper.mjs'
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms))
test('control windows measure a real reconnect and reject a later slot without one',{timeout:20000},async()=>{
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
    const oracle=join(directory,'oracle.mjs'),counter=join(directory,'head-called')
    writeFileSync(oracle,`import {existsSync,writeFileSync} from 'node:fs';const counter=${JSON.stringify(counter)};if(process.argv[2]==='head'){const next=existsSync(counter)?'2':'1';writeFileSync(counter,'1');console.log(JSON.stringify({log_id:'test',next,topics:['a']}))}else console.log(JSON.stringify({log_id:'test',start:'1',through:'1',events:[{id:'demo/trades/epoch/1',topic:'a',offset:'1',body:{price:'1'}}]}));`)
    writeFileSync(join(directory,'token'),'Bearer synthetic')
    const socketPath=join(directory,'control.sock')
    writeFileSync(join(directory,'config.json'),JSON.stringify({clients:1,workers:1,durationSeconds:10,requireReconnectSlo:true,urls:[`ws://127.0.0.1:${server.address().port}/experimental/ha/ws`],stateDirectory:join(directory,'state'),controlSocket:socketPath,output:join(directory,'report.json'),tokenFile:join(directory,'token'),headCommand:[process.execPath,oracle,'head'],rangeCommand:[process.execPath,oracle,'range']}))
    child=spawn(process.execPath,[fileURLToPath(new URL('./run.mjs',import.meta.url)),join(directory,'config.json')],{stdio:'ignore'})
    const exited=once(child,'exit')
    const call=(path,body)=>new Promise((resolve,reject)=>{
      const data=JSON.stringify(body),req=request({socketPath,path,method:'POST',headers:{'content-length':Buffer.byteLength(data)},timeout:5000},res=>{let text='';res.on('data',chunk=>text+=chunk);res.on('end',()=>{try{resolve(JSON.parse(text))}catch(error){reject(error)}});res.on('error',reject)})
      req.on('error',reject);req.on('timeout',()=>req.destroy(new Error('test control deadline')));req.end(data)
    })
    for(let i=0;i<100&&!existsSync(socketPath);i++)await delay(20)
    assert.ok(existsSync(socketPath))
    const first={window:'a',planHash:'plan',slot:'wts-gateway-a',rollback:false,nonce:'first'}
    assert.equal((await call('/begin',first)).ok,true)
    for(const socket of server.clients)socket.terminate()
    for(let i=0;i<100&&connections<2;i++)await delay(20)
    const verified=await call('/verify',first)
    assert.equal(verified.ok,true);assert.equal(verified.reconnect_samples,1)
    const second={...first,window:'b',slot:'wts-gateway-b',nonce:'second'}
    assert.equal((await call('/verify',second)).ok,false)
    assert.equal((await call('/begin',second)).ok,true)
    const empty=await call('/verify',second)
    assert.equal(empty.ok,false);assert.equal(empty.reconnect_slo_status,'not-measured')
    child.kill('SIGTERM');await exited
  } finally {
    if(child?.exitCode===null){child.kill('SIGKILL');await once(child,'exit')}
    for(const timer of timers)clearInterval(timer)
    for(const socket of server.clients)socket.terminate()
    await new Promise(resolve=>server.close(resolve));rmSync(directory,{recursive:true,force:true})
  }
})

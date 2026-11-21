import assert from 'node:assert/strict'
import { once } from 'node:events'
import test from 'node:test'
import { WebSocketServer } from '../edge/node_modules/ws/wrapper.mjs'
import { runClient } from './ha-client.mjs'
import { MemoryStore } from './ha-state.mjs'

async function server() {
  const server = new WebSocketServer({host:'127.0.0.1',port:0,handleProtocols:()=>'wts.ha.v1'})
  await once(server,'listening')
  return server
}
const options = server => ({ urls:[`ws://127.0.0.1:${server.address().port}/experimental/ha/ws`],token:()=> 'Bearer test',store:new MemoryStore('events',['a']),startAt:{log_id:'test',partition:0,offset:'0'} })
function close(server) { for (const client of server.clients) client.terminate(); server.close() }

test('three unchanged attempts without a first checkpoint stop',async () => {
  const mock = await server()
  let attempts = 0
  mock.on('connection', socket => {
    attempts++
    socket.on('message',bytes => {
      const frame = JSON.parse(bytes)
      if (frame.type === 'connect') socket.send('{"type":"connected"}')
      if (frame.type === 'resume') socket.send('{"type":"error","code":"replay-limit"}')
    })
  })
  const random = Math.random
  Math.random = () => 0
  try { await assert.rejects(runClient(options(mock)),/no-checkpoint-progress/); assert.equal(attempts,3) }
  finally { Math.random = random; close(mock) }
})

test('a silent server is detected at the receive deadline despite outgoing heartbeats',async () => {
  const mock = await server(), controller = new AbortController()
  let first, elapsed, heartbeats = 0
  mock.on('connection',socket => {
    if (first !== undefined) { elapsed = performance.now() - first; controller.abort(); return }
    first = performance.now()
    socket.on('message',bytes => {
      const frame = JSON.parse(bytes)
      if (frame.type === 'connect') socket.send('{"type":"connected"}')
      if (frame.type === 'heartbeat') heartbeats++
    })
  })
  const random = Math.random
  Math.random = () => 0
  try {
    await runClient({...options(mock),signal:controller.signal})
    assert.ok(elapsed >= 2900 && elapsed < 3700,`deadline elapsed ${elapsed}`)
    assert.ok(heartbeats >= 2)
  } finally { Math.random = random; close(mock) }
})

test('newly signed cursors at the same offset still count as no progress',async () => {
  const mock = await server()
  let attempts = 0
  mock.on('connection',socket => {
    attempts++
    socket.on('message',bytes => {
      const frame = JSON.parse(bytes)
      if (frame.type === 'connect') socket.send('{"type":"connected"}')
      if (frame.type === 'resume') {
        socket.send(JSON.stringify({type:'checkpoint',offset:'5',cursor:`new-signature-${attempts}`}))
        socket.send('{"type":"error","code":"replay-limit"}')
      }
    })
  })
  const args=options(mock), saved=args.store.load()
  saved.offset='5'; saved.cursor='previous-signature'; args.store.commit(saved)
  const random=Math.random; Math.random=()=>0
  try { await assert.rejects(runClient(args),/no-checkpoint-progress/); assert.equal(attempts,3); assert.equal(args.store.load().offset,'5') }
  finally { Math.random=random; close(mock) }
})

test('an asynchronous apply failure prevents queued checkpoints from advancing the cursor',async()=>{
  const mock=await server(),args=options(mock)
  const saved={...args.store.load(),offset:'5',cursor:'saved-5'};args.store.commit(saved)
  mock.on('connection',socket=>socket.on('message',bytes=>{
    const frame=JSON.parse(bytes)
    if(frame.type==='connect')socket.send('{"type":"connected"}')
    if(frame.type==='resume'){
      socket.send(JSON.stringify({type:'event',event_id:'demo/trades/epoch/6',topic:'a',position:{log_id:'test',partition:0,offset:'6'},body:{price:42}}))
      socket.send('{"type":"checkpoint","offset":"6","cursor":"must-not-save"}')
    }
  }))
  try{
    await assert.rejects(runClient({...args,async onFrame(frame){if(frame.type==='event'){await new Promise(resolve=>setTimeout(resolve,20));throw new Error('apply failed')}}}),/client-apply-failed/)
    assert.deepEqual(args.store.load(),saved)
  }finally{close(mock)}
})

test('transient admission failures before the first checkpoint remain retriable',async()=>{
  const mock=await server(),controller=new AbortController()
  let attempts=0
  mock.on('connection',socket=>{
    attempts++
    socket.on('message',bytes=>{
      const frame=JSON.parse(bytes)
      if(frame.type==='connect')socket.send('{"type":"connected"}')
      if(frame.type==='resume'){
        if(attempts<5)socket.send('{"type":"error","code":"retry-later"}')
        else controller.abort()
      }
    })
  })
  const random=Math.random;Math.random=()=>0
  try{await runClient({...options(mock),signal:controller.signal});assert.equal(attempts,5)}
  finally{Math.random=random;close(mock)}

})

test('durable apply backpressure pauses burst input and resumes without losing events',async()=>{
  const mock=await server(),controller=new AbortController(),args=options(mock)
  let attempts=0,pauses=0
  mock.on('connection',socket=>{
    attempts++
    socket.on('message',bytes=>{
      const frame=JSON.parse(bytes)
      if(frame.type==='connect')socket.send('{"type":"connected"}')
      if(frame.type==='resume'){
        for(let i=1;i<=2000;i++){
          socket.send(JSON.stringify({type:'event',event_id:`demo/trades/epoch/${i}`,topic:'a',position:{log_id:'test',partition:0,offset:String(i)},body:{padding:'x'.repeat(900)}}))
          if(i%100===0)socket.send(JSON.stringify({type:'checkpoint',offset:String(i),cursor:`cursor-${i}`}))
        }
      }
    })
  })
  const timeout=setTimeout(()=>controller.abort(),10000)
  try{
    await runClient({...args,signal:controller.signal,onAttempt(socket){const pause=socket.pause.bind(socket);socket.pause=()=>{pauses++;pause()}},async onCommit(state){await new Promise(resolve=>setTimeout(resolve,state.count==='100'?3600:10));if(state.count==='2000')controller.abort()}})
    assert.equal(args.store.load().count,'2000');assert.equal(attempts,1);assert.ok(pauses>0)
  }finally{clearTimeout(timeout);close(mock)}
})

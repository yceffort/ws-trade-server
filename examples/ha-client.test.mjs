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

const event = i => JSON.stringify({type:'event',event_id:`demo/trades/epoch/${i}`,topic:'a',position:{log_id:'test',partition:0,offset:String(i)},body:{}})
// 합성 서버의 connect와 resume 응답을 연결 번호별로 정한다. 받은 토큰과 resume을 기록한다.
async function scripted(onResume, onConnect = socket => socket.send('{"type":"connected"}')) {
  const mock = await server(), seen = {tokens:[],resumes:[],connections:0}
  mock.on('connection',socket => {
    const n = ++seen.connections
    socket.on('message',bytes => {
      const frame = JSON.parse(bytes)
      if (frame.type === 'connect') { seen.tokens.push(frame.token); onConnect(socket,n) }
      if (frame.type === 'resume') { seen.resumes.push(frame.cursor ?? null); onResume(socket,n) }
    })
  })
  return {mock,seen}
}
async function quickly(run) {
  const random = Math.random
  Math.random = () => 0
  try { return await run() } finally { Math.random = random }
}

test('an expired session refreshes its token and resumes from the persisted cursor without loss or duplicates',async()=>{
  const controller = new AbortController()
  const {mock,seen} = await scripted((socket,n) => {
    if (n === 1) { for (const i of [1,2,3]) socket.send(event(i)); socket.send('{"type":"checkpoint","offset":"3","cursor":"c3"}'); socket.send('{"type":"error","code":"auth-expired"}') }
    else { for (const i of [3,4,5]) socket.send(event(i)); socket.send('{"type":"checkpoint","offset":"5","cursor":"c5"}') }
  })
  const args = options(mock), tokens = ['Bearer old','Bearer new']
  let calls = 0
  try {
    await quickly(() => runClient({...args,token:()=>tokens[calls++],signal:controller.signal,onCommit(state){if(state.offset==='5')controller.abort()}}))
    const saved = args.store.load()
    assert.deepEqual(seen.tokens,tokens); assert.deepEqual(seen.resumes,[null,'c3']); assert.equal(calls,2)
    assert.equal(saved.count,'5'); assert.equal(saved.cursor,'c5')
  } finally { close(mock) }
})

test('the expired token is not reused and repeated refresh failures stop at the bound',async()=>{
  for (const fresh of [false,true]) {
    const {mock,seen} = await scripted(() => {},socket => socket.send('{"type":"error","code":"auth-expired"}'))
    const args = options(mock)
    let calls = 0
    try {
      await assert.rejects(quickly(() => runClient({...args,token:()=>fresh?`Bearer t${++calls}`:(calls++,'Bearer old')})),/auth-expired/)
      // 같은 토큰이면 처음 한 번만 접속하고, 매번 새 토큰이어도 만료가 이어지면 4번째 접속에서 멈춘다.
      assert.equal(seen.connections,fresh?4:1); assert.equal(calls,4); assert.equal(args.store.load().offset,null)
    } finally { close(mock) }
  }
})

test('token provider failures are retried while transient and stop when permanent',async()=>{
  const controller = new AbortController()
  const {mock,seen} = await scripted(socket => { socket.send(event(1)); socket.send('{"type":"checkpoint","offset":"1","cursor":"c1"}') })
  let calls = 0
  try {
    await quickly(() => runClient({...options(mock),signal:controller.signal,token(){if(calls++===0)throw new Error('provider-busy');return 'Bearer ok'},onCommit(){controller.abort()}}))
    assert.equal(calls,2); assert.equal(seen.connections,1)
    await assert.rejects(runClient({...options(mock),token(){throw new Error('auth-invalid')}}),/auth-invalid/)
    await assert.rejects(quickly(() => runClient({...options(mock),token(){throw new Error('provider-down')}})),/token-unavailable/)
    assert.equal(seen.connections,1)
  } finally { close(mock) }
})

test('revoked and out of scope sessions stop without refreshing and keep the saved cursor',async()=>{
  for (const code of ['auth-revoked','forbidden-scope']) {
    const {mock,seen} = await scripted(socket => socket.send(JSON.stringify({type:'error',code})))
    const args = options(mock), saved = args.store.load()
    saved.offset = '5'; saved.cursor = 'c5'; args.store.commit(saved)
    let calls = 0
    try {
      await assert.rejects(runClient({...args,token:()=>(calls++,'Bearer test')}),new RegExp(code))
      assert.equal(calls,1); assert.equal(seen.connections,1); assert.deepEqual(seen.resumes,['c5'])
      assert.equal(args.store.load().cursor,'c5')
    } finally { close(mock) }
  }
})

test('cancelling while the token provider waits returns the saved state without connecting',async()=>{
  const {mock,seen} = await scripted(() => {})
  const controller = new AbortController(), args = options(mock)
  setTimeout(() => controller.abort(),100)
  try {
    const state = await runClient({...args,signal:controller.signal,token:({signal})=>new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(new Error('cancelled'))))})
    assert.deepEqual(state,args.store.load()); assert.equal(seen.connections,0)
  } finally { close(mock) }
})

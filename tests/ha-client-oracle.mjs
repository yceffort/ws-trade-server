import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawn } from 'node:child_process'
import { runClient } from '../examples/ha-client.mjs'
import { FileStore } from '../examples/ha-state.mjs'

const fixture = JSON.parse(readFileSync(process.argv[2]))
const token = () => 'Bearer synthetic-protocol-test'
const startAt = { log_id: fixture.log_id, partition: 0, offset: fixture.start }
const timeout = setTimeout(() => { console.error('protocol oracle timed out'); process.exit(1) }, 90000)
if (process.argv[3] === 'crash') {
  const mode = process.argv[5]
  await runClient({ urls: [fixture.urls[0]], token, store: new FileStore(process.argv[4], mode, fixture.topics), startAt,
    onFrame(frame) {
      if (frame.type === 'error') console.error('server error:',frame.code)
      if (frame.type === (mode === 'events' ? 'event' : 'snapshot-chunk')) return new Promise(() => process.send({reached:true}, () => process.kill(process.pid, 'SIGKILL')))
    }
  })
  throw new Error('crash point not reached')
}
async function request(resume, expected) {
  const { default: WebSocket } = await import('../edge/node_modules/ws/wrapper.mjs')
  const socket = new WebSocket(fixture.urls[2], 'wts.ha.v1', { handshakeTimeout: 3000 })
  const frames = []
  const ping = setInterval(() => { if (socket.readyState === WebSocket.OPEN) socket.ping() },100)
  try {
    await new Promise((resolve,reject) => {
      const timer = setTimeout(() => reject(new Error('request timed out')),5000)
      const done = fn => value => { clearTimeout(timer); fn(value) }
      socket.on('error',done(reject))
      socket.on('open', () => { socket.ping(); socket.send(JSON.stringify({type:'connect',token:token()})) })
      socket.on('message', data => {
        const frame = JSON.parse(data)
        frames.push(frame)
        if (frame.type === 'connected') socket.send(JSON.stringify({type:'resume',request_id:'bounds',topics:fixture.topics,mode:'events',...resume}))
        else if (frame.type === 'heartbeat' && expected !== 'heartbeat-timeout') socket.send('{"type":"heartbeat"}')
        else if (frame.type === 'error' || frame.type === 'caught-up' && expected === 'caught-up') done(resolve)(frame)
      })
    }).then(frame => assert.equal(frame.type === 'error' ? frame.code : frame.type,expected))
    return frames
  } finally { clearInterval(ping); socket.terminate() }
}
const directory = mkdtempSync(join(tmpdir(), 'wts-ha-oracle-'))
async function crash(mode, path) {
  const child = spawn(process.execPath, [process.argv[1],process.argv[2],'crash',path,mode], { stdio: ['ignore','ignore','pipe','ipc'] })
  let error = '', reached = false
  child.on('message', message => { reached ||= message.reached === true })
  child.stderr.on('data', data => { error += data.toString().slice(0,4096) })
  const killed = setTimeout(() => child.kill('SIGKILL'),15000)
  try {
    const signal = await new Promise((resolve,reject) => { child.once('error',reject); child.once('exit',(code,signal) => code === null ? resolve(signal) : reject(new Error(error))) })
    assert.equal(signal,'SIGKILL')
    assert.ok(reached, 'child reached the intended frame before crashing')
    const saved = new FileStore(path,mode,fixture.topics).load()
    if (mode === 'state') assert.equal(saved.cursor,null)
    else if (saved.offset !== null) assert.ok(BigInt(saved.offset) < BigInt(fixture.start))
    assert.equal(saved.count,'0')
  } finally { clearTimeout(killed); child.kill('SIGKILL') }
}
try {
  const path = join(directory,'events.json')
  await crash('events',path)
  const applied = [], events = [], endpoints = new Set()
  let replayLimits = 0, firstEvent = false, firstCheckpoint = false
  const controller = new AbortController()
  const saved = await runClient({ urls: [fixture.urls[1],fixture.urls[2]], token, store: new FileStore(path,'events',fixture.topics), startAt, signal: controller.signal,
    onFrame(frame,socket) {
      endpoints.add(socket.url)
      if (frame.type === 'event') {
        if (firstEvent && !firstCheckpoint) throw new Error('first checkpoint missing')
        firstEvent = true
        events.push(frame.event_id)
      }
      if (frame.type === 'checkpoint' && firstEvent) firstCheckpoint = true
      if (frame.type === 'error' && frame.code === 'replay-limit') replayLimits++
    },
    onCommit(state,ids) { applied.push(...ids); if (BigInt(state.count) === BigInt(fixture.events.length)) controller.abort() }
  })
  const ids = fixture.events.map(event => event.id)
  assert.deepEqual(applied,ids)
  assert.equal(saved.count,String(ids.length))
  const digest = ids.reduce((digest,id) => createHash('sha256').update(`${digest}\n${id}`).digest('hex'),'0'.repeat(64))
  assert.equal(saved.digest,digest)
  assert.ok(replayLimits >= 1)
  assert.equal(endpoints.size,2)
  for (const topic of fixture.topics) {
    const last = fixture.events.findLast(event => event.topic === topic)
    assert.equal(saved.states[topic].event_id,last.id)
    assert.deepEqual(saved.states[topic].body,last.body)
  }
  await request({start_at:{...startAt,offset:'9223372036854775807'}},'start-out-of-range')
  await request({start_at:{...startAt,log_id:'wrong-log'}},'start-out-of-range')
  await request({cursor:saved.cursor,topics:[fixture.topics[0]]},'invalid-cursor')
  await request({cursor:`${saved.cursor}x`},'invalid-cursor')
  await request({cursor:saved.cursor,start_at:startAt},'invalid-resume')
  const tail = await request({start_at:{...startAt,offset:fixture.tail}},'caught-up')
  assert.equal(tail.filter(frame => frame.type === 'event').length,0)
  await request({start_at:{...startAt,offset:fixture.tail}},'heartbeat-timeout')
  const statePath = join(directory,'state.json')
  await crash('state',statePath)
  const stateController = new AbortController()
  const state = await runClient({ urls: [fixture.urls[2]],token,store:new FileStore(statePath,'state',fixture.topics),signal:stateController.signal,
    onCommit(){ stateController.abort() }
  })
  for (const topic of fixture.topics) {
    assert.equal(state.states[topic].event_id,saved.states[topic].event_id)
    assert.deepEqual(state.states[topic].body,saved.states[topic].body)
  }
  assert.equal(state.count,'0')
  console.log(JSON.stringify({applied:ids.length,crash_recovered:true,snapshot_recovered:true,replay_limits:replayLimits,digest}))
} finally { clearTimeout(timeout); rmSync(directory,{recursive:true,force:true}) }

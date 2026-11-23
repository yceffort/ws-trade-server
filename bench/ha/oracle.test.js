import test from 'node:test'
import assert from 'node:assert/strict'
import { compare,extend,hash,payloadHash,zero,percentile,reconnectMetrics } from './oracle.js'
const events=[{id:'source/epoch/1',topic:'a',offset:'4',body:{volume:'2',price:'10.00'}},{id:'source/epoch/2',topic:'b',offset:'7',body:{volume:'3'}},{id:'source/epoch/3',topic:'a',offset:'9',body:{volume:'5'}}]
const oracle={log_id:'log',start:'3',through:'12',events}
function client(topics=['a','b'],end='12'){
  const eligible=events.filter(e=>topics.includes(e.topic)&&BigInt(e.offset)<=BigInt(end))
  return {id:1,topics,log_id:'log',offset:end,count:String(eligible.length),digest:eligible.reduce((sum,e)=>hash(`${sum}\n${e.id}`),zero),payload_digest:eligible.reduce((sum,e)=>extend(sum,e.id,payloadHash(e.body)),zero)}
}
test('oracle compares authorized committed events despite offset holes and quiet topics',()=>{
  assert.equal(compare(oracle,[client(),client(['a'],'8'),client(['quiet'])]).ok,true)
  assert.equal(compare(oracle,[{...client(),offset:'13'}]).ok,false)
})
test('missing, duplicated, reordered or changed payload application fails',()=>{
  for(const changed of [{count:'2'},{count:'4'},{digest:zero},{payload_digest:zero},{failed:true}])assert.equal(compare(oracle,[{...client(),...changed}]).ok,false)
  const altered=structuredClone(oracle);altered.events[0].body.volume='99'
  assert.equal(compare(altered,[client()]).ok,false)
  assert.throws(()=>compare({...oracle,events:[events[1],events[0]]},[client()]),/sequence/)
})
test('canonical payloads ignore key ordering and SLO uses nearest rank',()=>{
  assert.equal(payloadHash({b:1,a:'2'}),payloadHash({a:'2',b:1}))
  assert.equal(percentile(Array.from({length:100},(_,i)=>i+1),.99),99)
  assert.equal(percentile([],.99),null)
})

test('planned reconnect SLO excludes injected faults and reports absent samples',()=>{
  const result=reconnectMetrics([{disconnect_ms:1000,scenario:null,window:'a'},{disconnect_ms:30000,scenario:'broker-quorum'}])
  assert.equal(result.reconnect_p99_ms,1000);assert.equal(result.fault_reconnect_p99_ms,30000)
  assert.equal(result.reconnect_slo_status,'pass')
  assert.equal(reconnectMetrics([]).reconnect_slo_status,'not-measured')
  assert.equal(reconnectMetrics([{disconnect_ms:6000,scenario:null,window:'a'}]).reconnect_slo_status,'fail')
})

test('a slow slot cannot borrow earlier fast samples or unmarked reconnects',()=>{
  const samples=[...Array.from({length:1000},()=>({disconnect_ms:1,scenario:null,window:'a'})),{disconnect_ms:6000,scenario:null,window:'b'},{disconnect_ms:1,scenario:null,window:null}]
  assert.equal(reconnectMetrics(samples,'b').reconnect_slo_status,'fail')
  assert.equal(reconnectMetrics(samples,'c').reconnect_slo_status,'not-measured')
  assert.equal(reconnectMetrics([{disconnect_ms:1,scenario:null,window:null}]).reconnect_slo_status,'not-measured')
})

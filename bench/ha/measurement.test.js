import test from 'node:test'
import assert from 'node:assert/strict'
import { Measurement,measuredTargets } from './measurement.js'
test('a disconnect keeps its original window through retries and mark changes',()=>{
  const measurement=new Measurement()
  measurement.disconnect(1);measurement.mark({window:'a'});measurement.disconnect(2)
  assert.deepEqual(measurement.caughtUp(3),{disconnect_ms:2,window:null,fault:null,scenario:null,fault_ms:null})
  measurement.mark({name:'broker',fault:'once',at:4});measurement.disconnect(5);measurement.mark(null)
  assert.deepEqual(measurement.caughtUp(10),{disconnect_ms:5,window:null,fault:'once',scenario:'broker',fault_ms:6})
  measurement.mark({window:'b'});measurement.disconnect(11)
  assert.deepEqual(measurement.caughtUp(12),{disconnect_ms:1,window:'b',fault:null,scenario:null,fault_ms:null})
})
test('target claims require measured rollout, rollback and feed failover',()=>{
  const report={ok:true,clients:1000,duration_ms:600000,reconnect_slo_status:'pass',actions:[{name:'feed-failover',result:{ok:true,feed_failover_ms:20000}}],rollout_verifications:[false,true].flatMap(rollback=>['a','b','c'].map(slot=>({slot,rollback,ok:true,reconnect_slo_status:'pass'})))}
  assert.equal(measuredTargets(report),true)
  for(const change of [{clients:999},{duration_ms:599999},{reconnect_slo_status:'not-measured'},{reconnect_slo_status:'fail'},{actions:[]},{actions:[{name:'feed-failover',result:{ok:true,feed_failover_ms:30001}}]},{rollout_verifications:report.rollout_verifications.slice(1)}])assert.equal(measuredTargets({...report,...change}),false)
})

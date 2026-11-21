import { createHash } from 'node:crypto'
export const zero='0'.repeat(64)
export const hash=value=>createHash('sha256').update(value).digest('hex')
export function canonical(value) {
  if(Array.isArray(value))return `[${value.map(canonical).join(',')}]`
  if(value!==null&&typeof value==='object')return `{${Object.keys(value).sort().map(key=>`${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`
  return JSON.stringify(value)
}
export const payloadHash=body=>hash(canonical(body))
export const extend=(previous,id,bodyHash)=>hash(`${previous}\n${id}\n${bodyHash}`)
const position=value=>{
  if(typeof value!=='string'||!/^(0|[1-9]\d*)$/.test(value))throw new Error('invalid oracle position')
  return BigInt(value)
}
export function compare(oracle,clients) {
  const start=position(oracle.start),through=position(oracle.through)
  if(start>through||!Array.isArray(oracle.events)||!clients.length)throw new Error('invalid oracle range')
  let previous=start-1n
  const ids=new Set()
  for(const event of oracle.events){
    const offset=position(event.offset)
    if(offset<start||offset>through||offset<=previous||ids.has(event.id))throw new Error('invalid committed event sequence')
    ids.add(event.id);previous=offset
  }
  const scopes=new Map(), failures=[]
  for(const client of clients){
    const scope=JSON.stringify(client.topics)
    let prefix=scopes.get(scope)
    if(!prefix){
      prefix=[{offset:start-1n,count:0,digest:zero,payload_digest:zero}]
      const allowed=new Set(client.topics)
      for(const event of oracle.events)if(allowed.has(event.topic)){
        const last=prefix.at(-1)
        prefix.push({offset:position(event.offset),count:last.count+1,digest:hash(`${last.digest}\n${event.id}`),payload_digest:extend(last.payload_digest,event.id,payloadHash(event.body))})
      }
      scopes.set(scope,prefix)
    }
    const end=client.offset===null?start-1n:position(client.offset)
    if(client.failed||end<start||end>through||client.log_id!==oracle.log_id){failures.push({client:client.id,reason:'incomplete-or-out-of-range'});continue}
    let low=0,high=prefix.length
    while(low+1<high){const mid=(low+high)>>1;if(prefix[mid].offset<=end)low=mid;else high=mid}
    const expected=prefix[low]
    if(BigInt(client.count)!==BigInt(expected.count)||client.digest!==expected.digest||client.payload_digest!==expected.payload_digest)failures.push({client:client.id,reason:'committed-application-mismatch',expected:expected.count,actual:client.count})
  }
  return {ok:failures.length===0,clients:clients.length,committed_events:oracle.events.length,failures}
}
export function percentile(values,fraction){
  if(!values.length)return null
  const sorted=[...values].sort((a,b)=>a-b)
  return sorted[Math.max(0,Math.ceil(sorted.length*fraction)-1)]
}
export function reconnectMetrics(samples,window) {
  const planned=samples.filter(s=>s.disconnect_ms!==null&&s.scenario===null&&typeof s.window==='string'&&(window===undefined||s.window===window))
  const fault=samples.filter(s=>s.disconnect_ms!==null&&s.scenario!==null)
  const p99=percentile(planned.map(s=>s.disconnect_ms),.99)
  return {reconnect_p99_ms:p99,reconnect_samples:planned.length,fault_reconnect_p99_ms:percentile(fault.map(s=>s.disconnect_ms),.99),fault_reconnect_samples:fault.length,reconnect_slo_status:p99===null?'not-measured':p99<=5000?'pass':'fail'}
}

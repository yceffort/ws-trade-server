import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { once } from 'node:events'
import { startMetrics } from './metrics-proxy.mjs'
test('metrics proxy never forwards management paths or mutations',async()=>{
  const paths=[]
  const backend=createServer((request,response)=>{paths.push([request.method,request.url]);response.end('wts_ha_gateway_ready 1\n')}).listen(0,'127.0.0.1')
  await once(backend,'listening')
  const proxy=startMetrics({backend:`http://127.0.0.1:${backend.address().port}/ha-metrics`,host:'127.0.0.1',port:0})
  await once(proxy,'listening')
  const root=`http://127.0.0.1:${proxy.address().port}`
  try{
    for(const [path,method] of [['/drain','POST'],['/metrics','POST'],['/metrics?path=/drain','GET'],['/readyz','GET']])assert.equal((await fetch(root+path,{method})).status,404)
    const response=await fetch(root+'/metrics')
    assert.equal(response.status,200);assert.equal(await response.text(),'wts_ha_gateway_ready 1\n')
    assert.deepEqual(paths,[['GET','/ha-metrics']])
  }finally{proxy.close();proxy.closeAllConnections();backend.close();backend.closeAllConnections()}
})

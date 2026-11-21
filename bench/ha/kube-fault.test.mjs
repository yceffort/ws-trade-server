import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer,request } from 'node:http'
import { requestBody } from './kube-fault.mjs'

test('DELETE transmits zero grace and identity preconditions in the HTTP body',async()=>{
  const expected={apiVersion:'v1',kind:'DeleteOptions',gracePeriodSeconds:0,preconditions:{uid:'synthetic-pod',resourceVersion:'123'}}
  let received
  const server=createServer(async(req,res)=>{
    let body='';for await(const chunk of req)body+=chunk
    received={method:req.method,body};res.end('{}')
  })
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve))
  try {
    const {data,headers}=requestBody('DELETE',expected)
    await new Promise((resolve,reject)=>{
      const req=request(`http://127.0.0.1:${server.address().port}/pod`,{method:'DELETE',headers},res=>{res.resume();res.on('end',resolve)})
      req.on('error',reject);req.end(data)
    })
    assert.equal(received.method,'DELETE')
    assert.deepEqual(JSON.parse(received.body),expected)
    assert.equal(requestBody('PATCH',{}).headers['content-type'],'application/merge-patch+json')
  } finally {await new Promise(resolve=>server.close(resolve))}
})

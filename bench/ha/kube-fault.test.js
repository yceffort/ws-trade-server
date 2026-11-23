import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer,request } from 'node:http'
import { requestBody,responseJson,faultPods,restoreReplicas,recoveredPods } from './kube-fault.js'

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

test('truncated Kubernetes responses reject so fault restoration can run',async()=>{
  const server=createServer((req,res)=>{res.writeHead(200,{'content-length':100});res.write('{');setTimeout(()=>res.destroy(),10)})
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve))
  try {
    let restored=false
    await assert.rejects((async()=>{try{await new Promise((resolve,reject)=>{const req=request(`http://127.0.0.1:${server.address().port}`,res=>responseJson(res,resolve,reject));req.on('error',reject);req.end()})}finally{restored=true}})())
    assert.equal(restored,true)
  }finally{await new Promise(resolve=>server.close(resolve))}
})
test('quorum restoration retries conflicts with fresh versions and checks identity',async()=>{
  let gets=0,patches=0
  const api=async(method,path,body)=>{
    if(method==='GET')return {metadata:{uid:'same',resourceVersion:String(++gets)}}
    assert.equal(body.metadata.resourceVersion,String(gets));assert.equal(body.spec.replicas,3)
    if(++patches<3)throw Object.assign(new Error('conflict'),{status:409})
    return {ok:true}
  }
  assert.deepEqual(await restoreReplicas(api,'broker','same'),{ok:true});assert.equal(patches,3)
  await assert.rejects(restoreReplicas(api,'broker','other'),/identity changed/)
  let attempts=0
  await assert.rejects(restoreReplicas(async method=>{if(method==='GET')return {metadata:{uid:'same'}};attempts++;throw Object.assign(new Error('conflict'),{status:409})},'broker','same'),/conflict/)
  assert.equal(attempts,5)
})
test('fault targets exclude terminated history but quorum waits for deletion',()=>{
  const pod=(name,phase,deletionTimestamp)=>({metadata:{name,deletionTimestamp},status:{phase}})
  const pods=[pod('failed','Failed'),pod('done','Succeeded'),pod('live','Running'),pod('deleting','Failed','now')]
  assert.deepEqual(faultPods(pods).map(p=>p.metadata.name),['live'])
  assert.deepEqual(faultPods(pods,true).map(p=>p.metadata.name),['live','deleting'])
})

test('fault completion waits for fresh stable replacement Pods',()=>{
  const now=Date.now(),old=new Set(['old'])
  const pod={metadata:{uid:'new'},status:{phase:'Running',conditions:[{type:'Ready',status:'True',lastTransitionTime:new Date(now-15000).toISOString()}]}}
  assert.equal(recoveredPods([pod],old,1,now),true)
  assert.equal(recoveredPods([pod],old,2,now),false)
  assert.equal(recoveredPods([{...pod,metadata:{uid:'old'}}],old,1,now),false)
  assert.equal(recoveredPods([{...pod,metadata:{uid:'new',deletionTimestamp:'now'}}],old,1,now),false)
  assert.equal(recoveredPods([pod],old,1,now-1),false)
})

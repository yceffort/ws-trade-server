import { request } from 'node:http'
try {
  const expected=JSON.parse(process.env.WTS_ROLLOUT_REQUEST)
  const body=JSON.stringify(expected)
  const response=await new Promise((resolve,reject)=>{
    const req=request({socketPath:process.env.WTS_HA_CONTROL_SOCKET,path:expected.phase==='begin'?'/begin':'/verify',method:'POST',headers:{'content-type':'application/json','content-length':Buffer.byteLength(body)},timeout:90000},res=>{
      let bytes=''
      res.on('data',chunk=>{bytes+=chunk;if(bytes.length>65536)req.destroy(new Error('verification output limit'))})
      res.on('error',reject)
      res.on('close',()=>{if(!res.complete)reject(new Error('verification response interrupted'))})
      res.on('end',()=>{try{res.statusCode===200?resolve(JSON.parse(bytes)):reject(new Error('verification rejected'))}catch{reject(new Error('invalid verification response'))}})
    })
    req.on('timeout',()=>req.destroy(new Error('verification deadline')))
    req.on('error',reject);req.end(body)
  })
  if(response.nonce!==expected.nonce||response.planHash!==expected.planHash||response.slot!==expected.slot||response.ok!==true)throw new Error('verification identity mismatch')
  console.log(JSON.stringify(response))
}catch{console.error('HA rollout verification failed');process.exitCode=1}

import { createServer } from 'node:https'
import { readFileSync } from 'node:fs'
const directory=process.env.WTS_POLICY_DIRECTORY??'/policy'
const policy=JSON.parse(readFileSync(`${directory}/policy.json`))
const server=createServer({key:readFileSync(`${directory}/key.pem`),cert:readFileSync(`${directory}/cert.pem`)},(request,response)=>{
  if(request.method!=='GET'||request.url!=='/policy'){response.writeHead(404).end();return}
  const now=Math.floor(Date.now()/1000)
  response.writeHead(200,{'content-type':'application/json','cache-control':'no-store'})
  response.end(JSON.stringify({generated_at:now,valid_until:now+55,policy}))
})
server.listen(8443,'0.0.0.0')
process.once('SIGTERM',()=>{server.closeAllConnections();server.close()})

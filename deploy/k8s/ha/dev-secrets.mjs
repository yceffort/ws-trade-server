import { readFileSync, writeFileSync, mkdirSync, existsSync, chmodSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { execFileSync } from 'node:child_process'
import { generateKeyPairSync } from 'node:crypto'
import { render } from './render.mjs'
import { clientToken } from './dev-token.mjs'

try {
  const settings=JSON.parse(readFileSync(process.argv[2]))
  render(settings)
  const directory=resolve(process.argv[3]??'deploy/local/ha-k8s')
  if(existsSync(directory)) throw new Error('destination already exists; keep its cluster identity and credentials')
  execFileSync(process.execPath,['deploy/ha/prepare.mjs'],{stdio:'ignore'})
  const base=resolve('deploy/local/ha'), shared=join(base,'shared'), privateDir=join(base,'private')
  mkdirSync(directory,{recursive:true,mode:0o700});chmodSync(directory,0o700)
  const put=(name,bytes)=>writeFileSync(join(directory,name),bytes,{mode:0o600})
  const openssl=args=>execFileSync('openssl',args,{stdio:'ignore'})
  const ca=readFileSync(join(shared,'ca.crt'))
  const issue=(name,dns)=>{
    const key=join(directory,`${name}.key`),csr=join(directory,`${name}.csr`),crt=join(directory,`${name}.crt`),ext=join(directory,`${name}.ext`)
    put(`${name}.ext`,`subjectAltName=${dns.map(d=>`DNS:${d}`).join(',')}\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=critical,CA:FALSE\n`)
    openssl(['req','-new','-newkey','rsa:2048','-nodes','-subj',`/CN=${name}`,'-keyout',key,'-out',csr])
    openssl(['x509','-req','-in',csr,'-CA',join(shared,'ca.crt'),'-CAkey',join(privateDir,'ca.key'),'-CAcreateserial','-days','7','-extfile',ext,'-out',crt])
    chmodSync(key,0o600)
    return {key:readFileSync(key),cert:readFileSync(crt)}
  }
  const objects=[]
  const secret=(name,files)=>objects.push({apiVersion:'v1',kind:'Secret',metadata:{name,namespace:settings.namespace},immutable:true,type:'Opaque',data:Object.fromEntries(Object.entries(files).map(([key,value])=>[key,Buffer.from(value).toString('base64')]))})
  const brokers={'ca.crt':ca,'cluster-id':readFileSync(join(shared,'cluster-id'))}
  const voter=[0,1,2].map(id=>`${id}@wts-broker-${id}.wts-broker:9093`).join(',')
  for(let id=0;id<3;id++) {
    const dns=[`wts-broker-${id}.wts-broker`,`wts-broker-${id}.wts-broker.${settings.namespace}.svc`,'localhost']
    const pair=issue(`wts-broker-${id+1}`,dns)
    brokers[`broker${id}.pem`]=Buffer.concat([pair.key,Buffer.from('\n'),pair.cert])
    const source=readFileSync(join(shared,`broker${id+1}.properties`),'utf8').trim().split('\n').map(line=>line.split(/=(.*)/s).slice(0,2))
    const properties=Object.fromEntries(source)
    Object.assign(properties,{'node.id':id,'controller.quorum.voters':voter,'listeners':'INTERNAL://:9092,CONTROLLER://:9093','advertised.listeners':`INTERNAL://wts-broker-${id}.wts-broker:9092`,'listener.security.protocol.map':'INTERNAL:SASL_SSL,CONTROLLER:SSL','ssl.keystore.location':`/config/broker${id}.pem`})
    delete properties['listener.name.external.plain.sasl.jaas.config']
    brokers[`broker${id}.properties`]=Object.entries(properties).map(([k,v])=>`${k}=${v}\n`).join('')
  }
  const jaas=brokers['broker0.properties'].match(/username="broker" password="([a-f0-9]+)"/)[1]
  brokers['health.properties']=`security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="${jaas}";\nssl.truststore.type=PEM\nssl.truststore.location=/config/ca.crt\n`
  secret(`wts-broker-${settings.version}`,brokers)
  secret(`wts-admin-${settings.version}`,{'ca.crt':ca,'admin.properties':readFileSync(join(shared,'admin.properties'),'utf8').replace('/config/ca.crt','/admin/ca.crt')})
  const client=role=>{
    const data=JSON.parse(readFileSync(join(base,`${role}.json`)))
    data.properties['bootstrap.servers']=[0,1,2].map(id=>`wts-broker-${id}.wts-broker:9092`).join(',')
    data.properties['ssl.ca.location']='/credentials/ca.crt'
    return JSON.stringify(data)
  }
  secret(`wts-feed-${settings.version}`,{'ca.crt':ca,'feed.json':client('feed')})
  const policyHost=`wts-policy.${settings.namespace}.svc`
  const policyPair=issue('wts-policy',[policyHost,'wts-policy'])
  const edgePair=issue('wts-gateway',['wts-gateway',`wts-gateway.${settings.namespace}.svc`,'localhost',...(settings.gatewayNames??[])])
  secret(`wts-tls-${settings.version}`,{'cert.pem':edgePair.cert,'key.pem':edgePair.key})
  const {publicKey,privateKey}=generateKeyPairSync('rsa',{modulusLength:2048})
  const jwk=publicKey.export({format:'jwk'})
  const now=Math.floor(Date.now()/1000)
  const source=render(settings).items.find(o=>o.kind==='ConfigMap').data['source.json']
  const topics=JSON.parse(source).topics
  const policy={version:'1',keys:[{...jwk,kid:'dev-1',alg:'RS256',use:'sig',retain_until:now+86400}],subjects:{'dev-client':{generation:'1',topics}},revoked:{}}
  secret(`wts-dev-policy-${settings.version}`,{'cert.pem':policyPair.cert,'key.pem':policyPair.key,'policy.json':JSON.stringify(policy)})
  const policyConfig={url:`https://${policyHost}/policy`,issuer:`https://${policyHost}`,audience:'wts-ha',ca_file:'/credentials/ca.crt',credential_file:null,max_token_seconds:3600}
  secret(`wts-gateway-${settings.version}`,{'ca.crt':ca,'gateway.json':client('gateway'),'policy-client.json':JSON.stringify(policyConfig),'cursor-keys.json':readFileSync(join(base,'cursor-keys.json'))})
  const token=clientToken(privateKey,policyConfig.issuer,topics,now)
  put('client-key.pem',privateKey.export({type:'pkcs8',format:'pem'}))
  secret(`wts-dev-client-${settings.version}`,{'ca.crt':ca,token})
  put('token',token);put('ca.crt',ca)
  put('secrets.json',JSON.stringify({apiVersion:'v1',kind:'List',items:objects},null,2))
  const securityContext={allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:['ALL']}}
  const labels={'app.kubernetes.io/part-of':'wts-ha','app.kubernetes.io/component':'dev-policy'}
  const namespace=settings.namespace
  const dev=[{apiVersion:'v1',kind:'ConfigMap',metadata:{name:'wts-dev-policy-script',namespace},data:{'server.mjs':readFileSync('deploy/k8s/ha/dev-policy.mjs','utf8')}},{apiVersion:'v1',kind:'Service',metadata:{name:'wts-policy',namespace},spec:{selector:labels,ports:[{port:443,targetPort:8443}]}},{apiVersion:'apps/v1',kind:'Deployment',metadata:{name:'wts-dev-policy',namespace},spec:{replicas:1,selector:{matchLabels:labels},template:{metadata:{labels},spec:{automountServiceAccountToken:false,securityContext:{runAsNonRoot:true,runAsUser:10001,runAsGroup:10001,fsGroup:10001,seccompProfile:{type:'RuntimeDefault'}},containers:[{name:'policy',image:settings.edgeImage,command:['node','/script/server.mjs'],securityContext,ports:[{containerPort:8443}],resources:{requests:{cpu:'50m',memory:'32Mi'},limits:{cpu:'250m',memory:'128Mi'}},readinessProbe:{tcpSocket:{port:8443},periodSeconds:2},volumeMounts:[{name:'script',mountPath:'/script',readOnly:true},{name:'policy',mountPath:'/policy',readOnly:true}]}],volumes:[{name:'script',configMap:{name:'wts-dev-policy-script'}},{name:'policy',secret:{secretName:`wts-dev-policy-${settings.version}`,defaultMode:0o440}}]}}}},{apiVersion:'networking.k8s.io/v1',kind:'NetworkPolicy',metadata:{name:'dev-policy',namespace},spec:{podSelector:{matchLabels:labels},policyTypes:['Ingress','Egress'],ingress:[{from:[{podSelector:{matchLabels:{'app.kubernetes.io/component':'gateway'}}}],ports:[{protocol:'TCP',port:8443}]}]}},{apiVersion:'networking.k8s.io/v1',kind:'NetworkPolicy',metadata:{name:'dev-policy-client',namespace},spec:{podSelector:{matchLabels:{'app.kubernetes.io/component':'gateway'}},policyTypes:['Egress'],egress:[{to:[{podSelector:{matchLabels:labels}}],ports:[{protocol:'TCP',port:8443}]}]}}]
  put('development.json',JSON.stringify({apiVersion:'v1',kind:'List',items:dev},null,2))
  console.log('합성 kind 전용 Secret과 정책 공급자를 준비했습니다. 출력 파일에는 개발 비밀 값이 있습니다.')
} catch(error){console.error(error.message);process.exitCode=1}

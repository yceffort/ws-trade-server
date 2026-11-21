import { readFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'
import { isIP } from 'node:net'
import { render } from '../../deploy/k8s/ha/render.mjs'
export function loadJob(s,options={}) {
  render(s)
  if(!/^wts-ha(?:-[a-z0-9-]+)?$/.test(s.namespace)||!/^[-\w./:]+@sha256:[a-f0-9]{64}$/.test(s.loadImage))throw new Error('dedicated namespace and pinned load image required')
  const namespace=s.namespace,name='wts-ha-load',labels={'app.kubernetes.io/part-of':'wts-ha','app.kubernetes.io/component':'ha-load'}
  const resource=(apiVersion,kind,name,fields)=>({apiVersion,kind,metadata:{name,namespace},...fields})
  const podPeer=component=>({podSelector:{matchLabels:{'app.kubernetes.io/part-of':'wts-ha','app.kubernetes.io/component':component}}})
  const config={clients:1000,workers:4,durationSeconds:600,recoveryDeadlineMs:5000,requireReconnectSlo:true,urls:[`wss://wts-gateway.${namespace}.svc/experimental/ha/ws`],environment:{kind:'in-cluster-service',physical_domains_verified:false,external_lb_verified:false},...options,
    stateDirectory:'/state/clients',controlSocket:'/state/control.sock',output:'/state/report.json',tokenFile:'/client/token',headCommand:['/app/ha-oracle','head','/credentials/gateway.json','/source/source.json'],rangeCommand:['/app/ha-oracle','range','/credentials/gateway.json','/source/source.json']}
  const duration=config.durationSeconds
  if(!Number.isInteger(duration)||duration<10||duration>900)throw new Error('durationSeconds must be an integer from 10 to 900')
  const fault=Boolean(config.enableFaults);delete config.enableFaults
  const external=config.externalEgress??[];delete config.externalEgress
  if(!Array.isArray(external)||external.length>16||external.some(rule=>{
    const [address,prefix,...extra]=typeof rule?.cidr==='string'?rule.cidr.split('/'):[]
    const family=address&&isIP(address)
    return !family||extra.length||!/^\d+$/.test(prefix)||Number(prefix)>(family===4?32:128)||!Number.isInteger(rule.port)||rule.port<1||rule.port>65535
  }))throw new Error('externalEgress requires valid CIDRs and TCP ports')
  if(options.urls?.some(value=>new URL(value).hostname!==`wts-gateway.${namespace}.svc`)&&!external.length)throw new Error('external endpoints require explicit externalEgress')
  if(!fault&&(config.actions??[]).length)throw new Error('fault actions require explicit enableFaults')
  const items=[resource('v1','ConfigMap',name,{data:{'load.json':JSON.stringify(config)}}),resource('v1','ServiceAccount',name,{automountServiceAccountToken:false})]
  const secret=(name,secretName)=>({name,secret:{secretName,defaultMode:0o440}})
  const volumes=[{name:'config',configMap:{name}},{name:'state',emptyDir:{sizeLimit:'2Gi'}},secret('client',`wts-dev-client-${s.version}`),secret('credentials',`wts-gateway-${s.version}`),{name:'source',configMap:{name:`wts-source-${s.version}`}}]
  const mounts=['config','state','client','credentials','source'].map(name=>({name,mountPath:`/${name}`,readOnly:name!=='state'}))
  if(fault){
    items.push(resource('rbac.authorization.k8s.io/v1','Role',name,{rules:[{apiGroups:[''],resources:['pods'],verbs:['get','list','delete']},{apiGroups:['apps'],resources:['statefulsets'],resourceNames:['wts-broker'],verbs:['get','patch']},{apiGroups:['coordination.k8s.io'],resources:['leases'],resourceNames:['wts-feed'],verbs:['get']}]}),resource('rbac.authorization.k8s.io/v1','RoleBinding',name,{roleRef:{apiGroup:'rbac.authorization.k8s.io',kind:'Role',name},subjects:[{kind:'ServiceAccount',name,namespace}]}))
    volumes.push({name:'chaos',projected:{defaultMode:0o440,sources:[{serviceAccountToken:{path:'token',expirationSeconds:3600}},{configMap:{name:'kube-root-ca.crt',items:[{key:'ca.crt',path:'ca.crt'}]}}]}})
    mounts.push({name:'chaos',mountPath:'/var/run/chaos',readOnly:true})
  }
  items.push(resource('batch/v1','Job',name,{spec:{backoffLimit:0,activeDeadlineSeconds:duration+600,template:{metadata:{labels},spec:{serviceAccountName:name,restartPolicy:'Never',automountServiceAccountToken:false,securityContext:{runAsNonRoot:true,runAsUser:10001,runAsGroup:10001,fsGroup:10001,seccompProfile:{type:'RuntimeDefault'}},containers:[{name:'load',image:s.loadImage,command:['sh','-c','node /app/bench/ha/run.mjs /config/load.json; result=$?; sleep 120; exit "$result"'],env:[{name:'NODE_OPTIONS',value:'--max-old-space-size=384'},{name:'NODE_EXTRA_CA_CERTS',value:'/client/ca.crt'},...(fault?[{name:'WTS_HA_ALLOW_FAULTS',value:'dedicated-test-namespace'}]:[])],securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:['ALL']}},volumeMounts:mounts,resources:{requests:{cpu:'1',memory:'512Mi'},limits:{cpu:'4',memory:'2Gi'}}}],volumes}}}}))
  const ports=(...numbers)=>numbers.map(port=>({protocol:'TCP',port}))
  items.push(resource('networking.k8s.io/v1','NetworkPolicy',name,{spec:{podSelector:{matchLabels:labels},policyTypes:['Ingress','Egress'],egress:[{to:[{namespaceSelector:{matchLabels:{'kubernetes.io/metadata.name':'kube-system'}},podSelector:{matchLabels:{'k8s-app':'kube-dns'}}}],ports:[...ports(53),{protocol:'UDP',port:53}]},{to:[podPeer('gateway')],ports:ports(8443)},{to:[podPeer('broker')],ports:ports(9092)},...external.map(rule=>({to:[{ipBlock:{cidr:rule.cidr}}],ports:ports(rule.port)})),...(fault?[{to:s.apiCIDRs.map(cidr=>({ipBlock:{cidr}})),ports:ports(443,6443)}]:[])]}}))
  for(const [component,port] of [['gateway',8443],['broker',9092]])items.push(resource('networking.k8s.io/v1','NetworkPolicy',`${name}-${component}`,{spec:{podSelector:podPeer(component).podSelector,policyTypes:['Ingress'],ingress:[{from:[{podSelector:{matchLabels:labels}}],ports:ports(port)}]}}))
  return {apiVersion:'v1',kind:'List',items}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){try{console.log(JSON.stringify(loadJob(JSON.parse(readFileSync(process.argv[2])),process.argv[3]?JSON.parse(readFileSync(process.argv[3])):{}),null,2))}catch(error){console.error(error.message);process.exitCode=1}}

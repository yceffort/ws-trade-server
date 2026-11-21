import { execFileSync } from 'node:child_process'
import { pathToFileURL } from 'node:url'
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms))
const [cluster,namespace,action]=process.argv.slice(2)
const run=(file,args)=>execFileSync(file,args,{encoding:'utf8',timeout:120000,maxBuffer:2*1024*1024})
export function faultNode(pods,loads) {
  const protectedNodes=new Set(loads.filter(p=>!['Failed','Succeeded'].includes(p.status.phase)).map(p=>p.spec.nodeName))
  const node=pods.find(p=>!protectedNodes.has(p.spec.nodeName))?.spec.nodeName
  if(!node)throw new Error('no gateway node without a load Pod')
  return node
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)try {
  if(!/^wts-ha-[a-z0-9-]+$/.test(cluster)||!/^wts-ha(?:-[a-z0-9-]+)?$/.test(namespace)||!['node-drain','network-partition'].includes(action)||process.env.WTS_HA_ALLOW_FAULTS!=='dedicated-test-namespace')throw new Error('dedicated kind cluster and opt-in required')
  const nodes=run('kind',['get','nodes','--name',cluster]).trim().split('\n')
  const base=['--context',`kind-${cluster}`,'--namespace',namespace,'--request-timeout=10s']
  const pods=JSON.parse(run('kubectl',[...base,'get','pods','-l','app.kubernetes.io/part-of=wts-ha,app.kubernetes.io/component=gateway','-o','json'])).items.filter(p=>!p.metadata.deletionTimestamp&&p.status.phase==='Running')
  if(pods.length!==3)throw new Error('three running gateways required')
  const loads=JSON.parse(run('kubectl',[...base,'get','pods','-l','app.kubernetes.io/part-of=wts-ha,app.kubernetes.io/component=ha-load','-o','json'])).items
  const node=faultNode(pods,loads)
  if(!nodes.includes(node)||node.endsWith('control-plane'))throw new Error('unexpected fault node')
  const started=Date.now()
  if(action==='node-drain'){
    try{run('kubectl',[...base,'drain',node,'--ignore-daemonsets','--delete-emptydir-data','--timeout=90s'])}
    finally{run('kubectl',[...base,'uncordon',node])}
  }else{
    const container=JSON.parse(run('docker',['inspect',node]))[0]
    const network=container.NetworkSettings.Networks.kind
    if(!network?.IPAddress)throw new Error('kind network address missing')
    try{
      run('docker',['network','disconnect','kind',node])
      await delay(15000)
    }finally{
      const current=JSON.parse(run('docker',['inspect',node]))[0].NetworkSettings.Networks.kind
      if(!current)run('docker',['network','connect','--ip',network.IPAddress,...(network.GlobalIPv6Address?['--ip6',network.GlobalIPv6Address]:[]),...(network.Aliases??[]).flatMap(a=>['--alias',a]),'kind',node])
    }
  }
  console.log(JSON.stringify({ok:true,action,node,elapsed_ms:Date.now()-started,physical_failure_domain_proof:false}))
}catch{console.error('kind fault failed; inspect the dedicated cluster and restore its node/network before retrying');process.exitCode=1}

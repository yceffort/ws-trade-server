import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
const [context,path]=process.argv.slice(2)
try {
  if(!context||!path) throw new Error('explicit context and settings required')
  const settings=JSON.parse(readFileSync(path))
  const {items}=JSON.parse(execFileSync('kubectl',['--context',context,'--request-timeout=15s','get','nodes','-o','json'],{encoding:'utf8',timeout:20000,maxBuffer:2*1024*1024}))
  const counts=Object.fromEntries(settings.zones.map(zone=>[zone,new Set(items.filter(node=>node.metadata.labels['topology.kubernetes.io/zone']===zone&&!node.spec.unschedulable&&!node.spec.taints?.some(t=>['NoSchedule','NoExecute'].includes(t.effect))&&node.status.conditions?.some(c=>c.type==='Ready'&&c.status==='True')).map(n=>n.metadata.labels['kubernetes.io/hostname'])).size]))
  if(Object.values(counts).some(n=>n<2)) throw new Error('each zone needs two ready eligible hostnames for surge')
  console.log(JSON.stringify({ok:true,context,zones:counts,scope:'scheduler eligibility only; capacity and physical isolation require separate verification'}))
} catch(error){console.error(error.message);process.exitCode=1}

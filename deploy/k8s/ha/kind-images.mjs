import { execFileSync } from 'node:child_process'
const [cluster,...images]=process.argv.slice(2)
try {
  if(!/^wts-ha-[a-z0-9-]+$/.test(cluster)||!images.length) throw new Error('dedicated wts-ha-* cluster and local images required')
  const nodes=execFileSync('kind',['get','nodes','--name',cluster],{encoding:'utf8'}).trim().split('\n')
  const result={}
  for(const image of images) {
    execFileSync('kind',['load','docker-image',image,'--name',cluster],{stdio:'ignore'})
    const canonical=image.includes('/')?image:`docker.io/library/${image}`
    let pinned
    for(const node of nodes) {
      const lines=execFileSync('docker',['exec',node,'ctr','--namespace','k8s.io','images','list'],{encoding:'utf8'}).split('\n')
      const columns=lines.find(line=>line.split(/\s+/)[0]===canonical)?.split(/\s+/)
      if(!columns||!/^sha256:[a-f0-9]{64}$/.test(columns[2])) throw new Error('imported image digest missing')
      const ref=`${canonical.replace(/:[^/:]+$/,'')}@${columns[2]}`
      if(pinned&&pinned!==ref) throw new Error('import digest differs between nodes')
      pinned=ref
      execFileSync('docker',['exec',node,'ctr','--namespace','k8s.io','images','tag','--force',canonical,pinned],{stdio:'ignore'})
    }
    result[image]=pinned
  }
  console.log(JSON.stringify(result))
} catch(error){console.error(error.message);process.exitCode=1}

import { execFileSync } from 'node:child_process'
import { pathToFileURL } from 'node:url'
export function canonicalImage(image) {
  if (typeof image!=='string' || !image || /\s/.test(image)) throw new Error('image reference required')
  const first=image.split('/')[0]
  if (!image.includes('/')) image=`docker.io/library/${image}`
  else if (!first.includes('.') && !first.includes(':') && first!=='localhost') image=`docker.io/${image}`
  if (!image.includes('@') && !image.split('/').at(-1).includes(':')) image+=':latest'
  return image
}
if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href) {
const [cluster,...images]=process.argv.slice(2)
try {
  if(!/^wts-ha-[a-z0-9-]+$/.test(cluster)||!images.length) throw new Error('dedicated wts-ha-* cluster and local images required')
  const nodes=execFileSync('kind',['get','nodes','--name',cluster],{encoding:'utf8'}).trim().split('\n')
  const result={}
  for(const image of images) {
    execFileSync('kind',['load','docker-image',image,'--name',cluster],{stdio:'ignore'})
    const canonical=canonicalImage(image)
    let pinned
    for(const node of nodes) {
      const lines=execFileSync('docker',['exec',node,'ctr','--namespace','k8s.io','images','list'],{encoding:'utf8'}).split('\n')
      const columns=lines.find(line=>line.split(/\s+/)[0]===canonical)?.split(/\s+/)
      if(!columns||!/^sha256:[a-f0-9]{64}$/.test(columns[2])) throw new Error('imported image digest missing')
      const ref=`${canonical.split('@')[0].replace(/:[^/:]+$/,'')}@${columns[2]}`
      if(pinned&&pinned!==ref) throw new Error('import digest differs between nodes')
      pinned=ref
      execFileSync('docker',['exec',node,'ctr','--namespace','k8s.io','images','tag','--force',canonical,pinned],{stdio:'ignore'})
    }
    result[image]=pinned
  }
  console.log(JSON.stringify(result))
} catch(error){console.error(error.message);process.exitCode=1}

}

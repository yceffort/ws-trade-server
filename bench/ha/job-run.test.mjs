import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,writeFileSync,readFileSync,rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'

test('a load Pod OOM still leaves container termination evidence',()=>{
  const directory=mkdtempSync(join(tmpdir(),'wts-job-evidence-'))
  try {
    const settings={...JSON.parse(readFileSync(new URL('../../deploy/k8s/ha/settings.example.json',import.meta.url))),loadImage:`example/load@sha256:${'1'.repeat(64)}`}
    const pod={metadata:{name:'wts-ha-load-test',uid:'test',labels:{'app.kubernetes.io/component':'ha-load'}},spec:{nodeName:'test-node'},status:{phase:'Failed',containerStatuses:[{name:'load',restartCount:0,state:{terminated:{reason:'OOMKilled',exitCode:137}}}]}}
    writeFileSync(join(directory,'kubectl'),`#!${process.execPath}\nconst args=process.argv.slice(2);if(args.includes('pods'))console.log(JSON.stringify(${JSON.stringify({items:[pod]})}));`,{mode:0o700})
    writeFileSync(join(directory,'settings.json'),JSON.stringify(settings))
    writeFileSync(join(directory,'options.json'),'{}')
    const output=join(directory,'results')
    const result=spawnSync(process.execPath,[fileURLToPath(new URL('./job-run.mjs',import.meta.url)),'test-context',join(directory,'settings.json'),join(directory,'options.json'),output],{env:{...process.env,PATH:`${directory}:${process.env.PATH}`},encoding:'utf8',timeout:10000})
    assert.equal(result.status,1)
    assert.match(result.stderr,/load pod exited without a collected report/)
    const saved=JSON.parse(readFileSync(join(output,'pods.json')))
    assert.equal(saved[0].containers[0].state.terminated.reason,'OOMKilled')
  } finally {rmSync(directory,{recursive:true,force:true})}
})

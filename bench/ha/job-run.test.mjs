import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,writeFileSync,readFileSync,rmSync,existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath,pathToFileURL } from 'node:url'
import { spawnSync } from 'node:child_process'

const pollingRun=(poll,check)=>{
  const directory=mkdtempSync(join(tmpdir(),'wts-job-poll-'))
  try {
    const settings={...JSON.parse(readFileSync(new URL('../../deploy/k8s/ha/settings.example.json',import.meta.url))),loadImage:`example/load@sha256:${'1'.repeat(64)}`}
    const pods=JSON.stringify({items:[{metadata:{name:'wts-ha-load-test',uid:'test',labels:{'app.kubernetes.io/component':'ha-load'}},spec:{nodeName:'test-node'},status:{phase:'Running',containerStatuses:[{name:'load',restartCount:0,state:{running:{}}}]}}]})
    writeFileSync(join(directory,'kubectl'),`#!/bin/sh\ncase "$*" in\n*component=ha-load*) ${poll}; echo '${pods}';;\n*exec*) echo '{"ok":true}';;\n*pods*) echo '${pods}';;\nesac\n`,{mode:0o700})
    // 5초 polling 간격과 실행 기한을 실제로 기다리지 않도록 시계만 앞당긴다.
    writeFileSync(join(directory,'clock.mjs'),'let now=Date.now();Date.now=()=>now;globalThis.setTimeout=(callback,ms)=>{now+=ms;return setImmediate(callback)}\n')
    writeFileSync(join(directory,'settings.json'),JSON.stringify(settings))
    writeFileSync(join(directory,'options.json'),JSON.stringify({durationSeconds:10}))
    const output=join(directory,'results')
    const result=spawnSync(process.execPath,['--import',pathToFileURL(join(directory,'clock.mjs')).href,fileURLToPath(new URL('./job-run.mjs',import.meta.url)),'test-context',join(directory,'settings.json'),join(directory,'options.json'),output],{env:{...process.env,PATH:`${directory}:${process.env.PATH}`},encoding:'utf8',timeout:10000})
    check(result,output,directory)
  } finally {rmSync(directory,{recursive:true,force:true})}
}

test('a transient polling failure still collects the report',()=>pollingRun('[ -e "$0.failed" ] || { : > "$0.failed"; exit 1; }',(result,output,directory)=>{
  assert.equal(result.status,0,result.stderr)
  assert.ok(existsSync(join(directory,'kubectl.failed')))
  assert.equal(JSON.parse(readFileSync(join(output,'report.json'))).ok,true)
}))

test('polling that fails until the deadline ends without a report',()=>pollingRun('echo >> "$0.calls"; exit 1',(result,output,directory)=>{
  assert.equal(result.status,1)
  assert.ok(readFileSync(join(directory,'kubectl.calls'),'utf8').split('\n').length>2)
  assert.match(result.stderr,/HA load cluster operation failed/)
  assert.equal(existsSync(join(output,'report.json')),false)
  assert.ok(existsSync(join(output,'pods.json')))
}))

for(const partialApply of [false,true])test(`diagnostics survive ${partialApply?'partial apply failure':'load Pod OOM'}`,()=>{
  const directory=mkdtempSync(join(tmpdir(),'wts-job-evidence-'))
  try {
    const settings={...JSON.parse(readFileSync(new URL('../../deploy/k8s/ha/settings.example.json',import.meta.url))),loadImage:`example/load@sha256:${'1'.repeat(64)}`}
    const pod={metadata:{name:'wts-ha-load-test',uid:'test',labels:{'app.kubernetes.io/component':'ha-load'}},spec:{nodeName:'test-node'},status:{phase:'Failed',containerStatuses:[{name:'load',restartCount:0,state:{terminated:{reason:'OOMKilled',exitCode:137}}}]}}
    writeFileSync(join(directory,'kubectl'),`#!${process.execPath}\nconst args=process.argv.slice(2);if(${partialApply}&&args.includes('apply')&&!args.includes('--dry-run=server'))process.exit(1);if(args.includes('pods'))console.log(JSON.stringify(${JSON.stringify({items:[pod]})}));`,{mode:0o700})
    writeFileSync(join(directory,'settings.json'),JSON.stringify(settings))
    writeFileSync(join(directory,'options.json'),'{}')
    const output=join(directory,'results')
    const result=spawnSync(process.execPath,[fileURLToPath(new URL('./job-run.mjs',import.meta.url)),'test-context',join(directory,'settings.json'),join(directory,'options.json'),output],{env:{...process.env,PATH:`${directory}:${process.env.PATH}`},encoding:'utf8',timeout:10000})
    assert.equal(result.status,1)
    assert.match(result.stderr,partialApply?/HA load cluster operation failed/:/load pod exited without a collected report/)
    const saved=JSON.parse(readFileSync(join(output,'pods.json')))
    assert.equal(saved[0].containers[0].state.terminated.reason,'OOMKilled')
  } finally {rmSync(directory,{recursive:true,force:true})}
})

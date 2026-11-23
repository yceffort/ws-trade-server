#!/usr/bin/env node
// 페이지뿐 아니라 워커(SharedWorker 포함) 안에서 열리는 웹소켓의 핸드셰이크와 프레임을 CDP로 받아 적는다.
// 의존성 없음(Node 22 이상의 내장 WebSocket 사용). 로그인하지 않은 임시 프로필로 띄운다.
// 사용: node capture/cdp-capture.js <url> [둘째 url] [--seconds 30] [--out evidence/raw/frames.jsonl] [--headed]
import { spawn } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    seconds: { type: 'string', default: '30' },
    out: { type: 'string', default: 'evidence/raw/frames.jsonl' },
    headed: { type: 'boolean', default: false },
    chrome: {
      type: 'string',
      default: process.env.CHROME ?? '/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta',
    },
  },
})
const url = positionals[0]
if (!url) {
  console.error('usage: node capture/cdp-capture.js <url> [--seconds 30] [--out file.jsonl] [--headed]')
  process.exit(1)
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const profile = mkdtempSync(join(tmpdir(), 'ws-capture-'))
const chrome = spawn(
  values.chrome,
  [
    '--remote-debugging-port=0',
    `--user-data-dir=${profile}`,
    '--no-first-run',
    '--no-default-browser-check',
    ...(values.headed ? [] : ['--headless=new']),
    'about:blank',
  ],
  { stdio: 'ignore' },
)

const portFile = join(profile, 'DevToolsActivePort')
for (let i = 0; i < 100 && !(existsSync(portFile) && readFileSync(portFile, 'utf8').includes('\n')); i++) await sleep(100)
const [port, path] = readFileSync(portFile, 'utf8').trim().split('\n')
const cdp = new WebSocket(`ws://127.0.0.1:${port}${path}`)
await new Promise((resolve, reject) => {
  cdp.onopen = resolve
  cdp.onerror = reject
})

let nextId = 0
const pending = new Map()
const send = (method, params = {}, sessionId) =>
  new Promise((resolve) => {
    const id = ++nextId
    pending.set(id, resolve)
    cdp.send(JSON.stringify({ id, method, params, sessionId }))
  })

const autoAttach = { autoAttach: true, waitForDebuggerOnStart: true, flatten: true }
const targets = new Map()
const records = []
const startedAt = Date.now()

cdp.onmessage = ({ data }) => {
  const msg = JSON.parse(data)
  if (msg.id) {
    pending.get(msg.id)?.(msg)
    pending.delete(msg.id)
    return
  }
  if (msg.method === 'Target.attachedToTarget') {
    const { sessionId, targetInfo } = msg.params
    targets.set(sessionId, { type: targetInfo.type, url: targetInfo.url })
    // 워커가 첫 줄을 실행하기 전에 Network를 켜야 핸드셰이크부터 잡힌다
    send('Network.enable', {}, sessionId)
    send('Target.setAutoAttach', autoAttach, sessionId)
    send('Runtime.runIfWaitingForDebugger', {}, sessionId)
    return
  }
  if (msg.method?.startsWith('Network.webSocket')) {
    records.push({
      ms: Date.now() - startedAt,
      target: targets.get(msg.sessionId),
      event: msg.method.slice('Network.'.length),
      ...msg.params,
    })
  }
}

const seconds = Number(values.seconds)
const mark = (step) => records.push({ ms: Date.now() - startedAt, event: 'scenario', step })

await send('Target.setAutoAttach', autoAttach)
await send('Target.createTarget', { url })
mark(`open ${url}`)
// 둘째 URL을 주면 1/3 지점에 새 탭으로 열고 2/3 지점에 닫는다. 탭이 사라질 때 워커가 구독을 어떻게 정리하는지 보기 위해서다.
const second = positionals[1]
if (second) {
  await sleep((seconds * 1000) / 3)
  const { result } = await send('Target.createTarget', { url: second })
  mark(`open ${second}`)
  await sleep((seconds * 1000) / 3)
  await send('Target.closeTarget', { targetId: result.targetId })
  mark(`close ${second}`)
  await sleep((seconds * 1000) / 3)
} else {
  await sleep(seconds * 1000)
}

writeFileSync(values.out, records.map((record) => JSON.stringify(record)).join('\n') + '\n')
const counts = {}
for (const { event, target } of records) {
  const key = `${target?.type ?? '?'} ${event}`
  counts[key] = (counts[key] ?? 0) + 1
}
console.log(`targets: ${[...new Set([...targets.values()].map((t) => t.type))].join(', ')}`)
console.log(`records: ${records.length} -> ${values.out}`)
console.table(counts)

await Promise.race([send('Browser.close'), sleep(2000)])
chrome.kill()
await sleep(300)
rmSync(profile, { recursive: true, force: true })
process.exit(0)

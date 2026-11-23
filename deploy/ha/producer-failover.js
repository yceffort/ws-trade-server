import assert from 'node:assert/strict'
import { spawn, execFile } from 'node:child_process'
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

const exec = promisify(execFile)
const root = fileURLToPath(new URL('../../', import.meta.url))
const binary = resolve(root, process.env.WTS_HA_BIN ?? 'target/debug/ha-feed')
const config = join(root, 'deploy/local/ha')
const temp = mkdtempSync(join(tmpdir(), 'wts-ha-lease-'))
const children = []
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
const ended = child => child.exitCode !== null || child.signalCode !== null
function start(command, args, env = {}) {
  const child = spawn(command, args, {
    cwd: root,
    env: { ...process.env, RUST_LOG: 'info', ...env },
    stdio: ['ignore', 'pipe', 'pipe']
  })
  child.output = ''
  const collect = data => {
    child.output = (child.output + data.toString()).slice(-65536)
  }
  child.stdout.on('data', collect)
  child.stderr.on('data', collect)
  child.on('error', error => {
    child.failure = error
  })
  children.push(child)
  return child
}
async function until(predicate, timeout, message) {
  const deadline = performance.now() + timeout
  while (performance.now() < deadline) {
    if (await predicate()) return
    await sleep(50)
  }
  throw new Error(message)
}
async function active(child, timeout) {
  await until(
    () => {
      if (child.failure || ended(child)) throw new Error(`producer exited: ${child.output}`)
      return child.output.includes('ha_producer_active')
    },
    timeout,
    'producer activation deadline'
  )
}
async function audit() {
  const result = await exec(
    binary,
    ['audit', '--kafka', join(config, 'feed.json'), '--source', join(config, 'source.json')],
    { cwd: root, timeout: 40000, maxBuffer: 1024 * 1024 }
  )
  return JSON.parse(result.stdout.trim())
}
try {
  const api = start(process.execPath, ['deploy/ha/dev-lease.js'], { WTS_LEASE_PORT: '0' })
  await until(() => api.output.includes('lease_api_started'), 5000, 'lease API startup deadline')
  const port = JSON.parse(api.output.trim().split('\n')[0]).port
  const leases = ['demo-a', 'demo-b'].map(identity => {
    const lease = JSON.parse(readFileSync(join(config, `lease-${identity}.json`), 'utf8'))
    lease.url = lease.url.replace(':18081/', `:${port}/`)
    const path = join(temp, `${identity}.json`)
    writeFileSync(path, JSON.stringify(lease), { mode: 0o600 })
    return path
  })
  const command = lease => [
    'run',
    '--kafka',
    join(config, 'feed.json'),
    '--source',
    join(config, 'source.json'),
    '--lease',
    lease
  ]
  const first = start(binary, command(leases[0]))
  await active(first, 30000)
  const second = start(binary, command(leases[1]))
  await sleep(1500)
  assert(!second.output.includes('ha_producer_active'), 'standby must not fence a healthy writer')
  const before = await audit()
  const stoppedAt = performance.now()
  first.kill('SIGSTOP')
  await active(second, 30000)
  await sleep(300)
  const after = await audit()
  const failoverMs = Math.round(performance.now() - stoppedAt)
  assert(BigInt(after.source_seq) > BigInt(before.source_seq))
  assert(failoverMs <= 30000, `failover ${failoverMs}ms exceeds target`)
  first.kill('SIGCONT')
  await until(() => ended(first), 10000, 'resumed stale writer did not stop')
  assert.notEqual(first.exitCode, 0)
  api.kill('SIGTERM')
  await until(() => ended(second), 8000, 'writer did not stop after API loss')
  assert.notEqual(second.exitCode, 0)
  const final = await audit()
  await sleep(1500)
  assert.equal((await audit()).source_seq, final.source_seq)
  console.log(
    JSON.stringify({
      result: 'pass',
      failover_ms: failoverMs,
      committed_events: final.events,
      stale_writer_stopped: true,
      api_loss_stopped: true
    })
  )
} finally {
  for (const child of children)
    if (!ended(child)) {
      child.kill('SIGCONT')
      child.kill('SIGTERM')
    }
  await sleep(300)
  for (const child of children) if (!ended(child)) child.kill('SIGKILL')
  rmSync(temp, { recursive: true, force: true })
}

// HA 기능 시험이 함께 쓰는 로컬 구성이다. Lease API, active와 standby feed, HA 프로토콜 gateway를 띄운다.
// smoke.sh가 만든 Compose 프로젝트와 deploy/local/ha 설정을 쓴다.
import { spawn, execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs'
import { randomBytes } from 'node:crypto'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

export const root = fileURLToPath(new URL('../../', import.meta.url))
export const config = join(root, 'deploy/local/ha')
export const bin = name => join(root, process.env.WTS_HA_BIN_DIR ?? 'target/debug', name)
export const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
export const ended = child => child.exitCode !== null || child.signalCode !== null
export const docker = args => execFileSync('docker', args, { cwd: root, encoding: 'utf8' })
export const compose = args => docker(['compose', '-f', 'deploy/ha/compose.yaml', ...args])
export const admin = ['--bootstrap-server', 'kafka1:9092', '--command-config', '/config/admin.properties']
export const oracle = command => [bin('ha-oracle'), command, join(config, 'gateway.json'), join(config, 'source.json')]

export function ownedProject() {
  const project = process.env.COMPOSE_PROJECT_NAME
  if (!/^wts-ha-test-[a-z0-9-]+$/.test(project ?? '') || process.env.WTS_HA_ALLOW_FAULTS !== 'owned-test-cluster') throw new Error('owned test Compose project required')
  return project
}

export async function until(predicate, timeout, message) {
  const deadline = performance.now() + timeout
  while (performance.now() < deadline) {
    if (await predicate()) return
    await sleep(100)
  }
  throw new Error(message)
}

export async function launch({ gateways: count }) {
  const temp = mkdtempSync(join(tmpdir(), 'wts-ha-stack-'))
  const children = []
  const start = (command, args, env = {}) => {
    const child = spawn(command, args, { cwd: root, env: { ...process.env, RUST_LOG: 'info', ...env }, stdio: ['ignore', 'pipe', 'pipe'] })
    child.output = ''
    const collect = data => { child.output = (child.output + data.toString()).slice(-262144) }
    child.stdout.on('data', collect)
    child.stderr.on('data', collect)
    child.once('error', error => { child.failure = error })
    children.push(child)
    return child
  }
  // 실행 파일이 없는 등 자식이 시작하지 못하면 기한을 기다리지 않고 그 오류로 끝낸다.
  const wait = (predicate, timeout, message) => until(() => {
    const failed = children.find(child => child.failure)
    if (failed) throw failed.failure
    return predicate()
  }, timeout, message)
  const stop = async () => {
    for (const child of children) if (!ended(child)) child.kill('SIGTERM')
    await sleep(500)
    for (const child of children) if (!ended(child)) child.kill('SIGKILL')
    rmSync(temp, { recursive: true, force: true })
  }
  try {
    const token = randomBytes(32).toString('hex')
    writeFileSync(join(temp, 'auth.json'), JSON.stringify([{ token, principal: 'ha-test', expires_at: Math.floor(Date.now() / 1000) + 3600 }]), { mode: 0o600 })
    writeFileSync(join(temp, 'token'), `Bearer ${token}`, { mode: 0o600 })
    const api = start(process.execPath, ['deploy/ha/dev-lease.js'], { WTS_LEASE_PORT: '0' })
    await wait(() => api.output.includes('lease_api_started'), 5000, 'lease API startup deadline')
    const port = JSON.parse(api.output.trim().split('\n')[0]).port
    const feeds = ['demo-a', 'demo-b'].map(identity => {
      const lease = JSON.parse(readFileSync(join(config, `lease-${identity}.json`), 'utf8'))
      lease.url = lease.url.replace(':18081/', `:${port}/`)
      writeFileSync(join(temp, `${identity}.json`), JSON.stringify(lease), { mode: 0o600 })
      return start(bin('ha-feed'), ['run', '--kafka', join(config, 'feed.json'), '--source', join(config, 'source.json'), '--lease', join(temp, `${identity}.json`)])
    })
    await wait(() => feeds.some(feed => feed.output.includes('ha_producer_active')), 30000, 'producer activation deadline')
    const gateways = Array.from({ length: count }, () => start(bin('gateway'), ['--ha-kafka', join(config, 'gateway.json'), '--ha-source', join(config, 'source.json'), '--ha-cursor-keys', join(config, 'cursor-keys.json'), '--auth-file', join(temp, 'auth.json'), '--addr', '127.0.0.1:0', '--http-addr', '127.0.0.1:0']))
    const address = (gateway, key) => gateway.output.match(new RegExp(`${key}=(?:Some\\()?([0-9.]+:\\d+)`))?.[1]
    await wait(() => gateways.every(g => address(g, 'http_addr')), 10000, 'gateway startup deadline')
    const ready = async g => (await fetch(`http://${address(g, 'http_addr')}/readyz`).catch(() => ({ ok: false }))).ok
    await wait(async () => (await Promise.all(gateways.map(ready))).every(Boolean), 30000, 'gateway readiness deadline')
    const urls = gateways.map(g => `ws://${address(g, 'addr')}/experimental/ha/ws`)
    return { temp, token: `Bearer ${token}`, tokenFile: join(temp, 'token'), feeds, gateways, urls, start, stop }
  } catch (error) {
    await stop()
    throw error
  }
}

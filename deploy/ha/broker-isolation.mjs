// broker 한 대를 예고 없이 잃는 동안 기존 클라이언트가 끊기지 않고 committed 기록과 같은 상태를 적용하는지 본다(#94).
// smoke.sh가 만든 Compose 프로젝트에서 `node deploy/ha/broker-isolation.mjs`로 실행한다. `fault <초>`는 run.mjs가 부르는 장애 action이다.
import assert from 'node:assert/strict'
import { spawn, execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs'
import { randomBytes } from 'node:crypto'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../../', import.meta.url))
const config = join(root, 'deploy/local/ha')
const bin = name => join(root, process.env.WTS_HA_BIN_DIR ?? 'target/debug', name)
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
const project = process.env.COMPOSE_PROJECT_NAME
if (!/^wts-ha-test-[a-z0-9-]+$/.test(project ?? '') || process.env.WTS_HA_ALLOW_FAULTS !== 'owned-test-cluster') throw new Error('owned test Compose project required')
const docker = args => execFileSync('docker', args, { cwd: root, encoding: 'utf8' })
const compose = args => docker(['compose', '-f', 'deploy/ha/compose.yaml', ...args])
const admin = ['--bootstrap-server', 'kafka1:9092', '--command-config', '/config/admin.properties']

if (process.argv[2] === 'fault') {
  // 체결 partition의 leader를 끊는다. AWS의 AZ 차단에서 feed가 재시작한 경우와 같은 조건이다.
  const describe = compose(['exec', '-T', 'kafka1', '/opt/kafka/bin/kafka-topics.sh', ...admin, '--describe', '--topic', 'wts.events.v1'])
  const broker = `kafka${describe.match(/Leader: (\d)/)[1]}`
  const container = compose(['ps', '-q', broker]).trim()
  const network = `${project}_default`
  const attached = JSON.parse(docker(['inspect', '--format', '{{json .NetworkSettings.Networks}}', container]))[network]
  const started = performance.now()
  docker(['network', 'disconnect', network, container])
  await sleep(Number(process.argv[3]) * 1000)
  try {
    // 같은 IP로 복원해 다른 broker의 DNS cache가 다른 인증서를 가진 피어를 가리키지 않게 한다.
    docker(['network', 'connect', '--alias', broker, '--ip', attached.IPAddress, network, container])
  } catch {
    // Docker 20.10은 Compose 기본 network에 IP 지정을 거절한다. 그때는 alias만 복원한다.
    docker(['network', 'connect', '--alias', broker, network, container])
  }
  console.log(JSON.stringify({ ok: true, broker, isolated_ms: Math.round(performance.now() - started) }))
  process.exit(0)
}

const temp = mkdtempSync(join(tmpdir(), 'wts-ha-isolation-'))
const children = []
const ended = child => child.exitCode !== null || child.signalCode !== null
function start(command, args, env = {}) {
  const child = spawn(command, args, { cwd: root, env: { ...process.env, RUST_LOG: 'info', ...env }, stdio: ['ignore', 'pipe', 'pipe'] })
  child.output = ''
  const collect = data => { child.output = (child.output + data.toString()).slice(-262144) }
  child.stdout.on('data', collect)
  child.stderr.on('data', collect)
  children.push(child)
  return child
}
async function until(predicate, timeout, message) {
  const deadline = performance.now() + timeout
  while (performance.now() < deadline) {
    if (await predicate()) return
    await sleep(100)
  }
  throw new Error(message)
}
try {
  const token = randomBytes(32).toString('hex')
  writeFileSync(join(temp, 'auth.json'), JSON.stringify([{ token, principal: 'isolation-test', expires_at: Math.floor(Date.now() / 1000) + 3600 }]), { mode: 0o600 })
  writeFileSync(join(temp, 'token'), `Bearer ${token}`, { mode: 0o600 })
  const api = start(process.execPath, ['deploy/ha/dev-lease.mjs'], { WTS_LEASE_PORT: '0' })
  await until(() => api.output.includes('lease_api_started'), 5000, 'lease API startup deadline')
  const port = JSON.parse(api.output.trim().split('\n')[0]).port
  const feeds = ['demo-a', 'demo-b'].map(identity => {
    const lease = JSON.parse(readFileSync(join(config, `lease-${identity}.json`), 'utf8'))
    lease.url = lease.url.replace(':18081/', `:${port}/`)
    writeFileSync(join(temp, `${identity}.json`), JSON.stringify(lease), { mode: 0o600 })
    return start(bin('ha-feed'), ['run', '--kafka', join(config, 'feed.json'), '--source', join(config, 'source.json'), '--lease', join(temp, `${identity}.json`)])
  })
  await until(() => feeds.some(feed => feed.output.includes('ha_producer_active')), 30000, 'producer activation deadline')
  const gateways = [0, 1].map(() => start(bin('gateway'), ['--ha-kafka', join(config, 'gateway.json'), '--ha-source', join(config, 'source.json'), '--ha-cursor-keys', join(config, 'cursor-keys.json'), '--auth-file', join(temp, 'auth.json'), '--addr', '127.0.0.1:0', '--http-addr', '127.0.0.1:0']))
  const address = (gateway, key) => gateway.output.match(new RegExp(`${key}=(?:Some\\()?([0-9.]+:\\d+)`))?.[1]
  await until(() => gateways.every(g => address(g, 'http_addr')), 10000, 'gateway startup deadline')
  const ready = async g => (await fetch(`http://${address(g, 'http_addr')}/readyz`).catch(() => ({ ok: false }))).ok
  await until(async () => (await Promise.all(gateways.map(ready))).every(Boolean), 30000, 'gateway readiness deadline')
  const oracle = command => [bin('ha-oracle'), command, join(config, 'gateway.json'), join(config, 'source.json')]
  const settings = {
    clients: 20, workers: 2, durationSeconds: 70, startupIntervalMs: 20, requireReconnectSlo: false,
    urls: gateways.map(g => `ws://${address(g, 'addr')}/experimental/ha/ws`), tokenFile: join(temp, 'token'),
    stateDirectory: join(temp, 'clients'), output: join(temp, 'report.json'), headCommand: oracle('head'), rangeCommand: oracle('range'),
    environment: { kind: 'single-host-compose', physical_domains_verified: false, external_lb_verified: false },
    actions: [{ name: 'isolate-events-leader', atSeconds: 15, command: [process.execPath, fileURLToPath(import.meta.url), 'fault', '25'] }],
  }
  writeFileSync(join(temp, 'load.json'), JSON.stringify(settings), { mode: 0o600 })
  const load = start(process.execPath, ['bench/ha/run.mjs', join(temp, 'load.json')])
  await until(() => ended(load), 240000, 'load run deadline')
  const report = JSON.parse(readFileSync(join(temp, 'report.json'), 'utf8'))
  const disconnects = Object.values(report.disconnect_reasons ?? {}).reduce((a, b) => a + b, 0)
  const found = (processes, pattern) => processes.flatMap(child => [...child.output.matchAll(pattern)].map(match => match.slice(1).join(' ')))
  const summary = {
    result: report.ok && disconnects === 0 ? 'pass' : 'fail', committed_match: report.comparison?.ok ?? false,
    disconnect_reasons: report.disconnect_reasons ?? null, action: report.actions?.[0]?.result ?? null,
    feed_stops: found(feeds, /ha_producer_stopped" reason=(\S+)/g), replica_failures: found(gateways, /ha_replica_failed" reason=(\S+) code=(\S+)/g),
  }
  console.log(JSON.stringify(summary))
  // 다음 시험이 broker를 하나 더 내리므로 끊었던 broker가 모든 ISR에 돌아온 뒤 끝낸다.
  const isr = () => compose(['exec', '-T', 'kafka1', '/opt/kafka/bin/kafka-topics.sh', ...admin, '--describe', '--topic', 'wts.events.v1,wts.checkpoint.v1,wts.snapshot.v1,__transaction_state'])
  const rejoined = () => { try { return [...isr().matchAll(/Isr: ([\d,]+)/g)].every(match => match[1].split(',').length === 3) } catch { return false } }
  await until(rejoined, 60000, 'isolated broker did not rejoin ISR')
  if (summary.result !== 'pass') {
    // 원인을 가릴 수 있게 feed와 gateway의 경고 이상 로그 끝부분만 남긴다. 인증 자료는 로그에 나오지 않는다.
    for (const [name, child] of [['feed-a', feeds[0]], ['feed-b', feeds[1]], ['gateway-0', gateways[0]], ['gateway-1', gateways[1]]]) {
      console.error(`# ${name}\n${child.output.split('\n').filter(line => / (WARN|ERROR) /.test(line) && !line.includes('broker_client_error')).slice(-20).join('\n')}`)
    }
  }
  assert.equal(report.ok, true, `load report failed: ${report.reason ?? 'comparison'}`)
  assert.equal(disconnects, 0, 'existing clients disconnected while one broker was isolated')
} finally {
  for (const child of children) if (!ended(child)) child.kill('SIGTERM')
  await sleep(500)
  for (const child of children) if (!ended(child)) child.kill('SIGKILL')
  rmSync(temp, { recursive: true, force: true })
}

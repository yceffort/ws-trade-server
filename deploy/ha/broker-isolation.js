// broker 한 대를 예고 없이 잃는 동안 기존 클라이언트가 끊기지 않고 committed 기록과 같은 상태를 적용하는지 본다(#94).
// smoke.sh가 만든 Compose 프로젝트에서 `node deploy/ha/broker-isolation.js`로 실행한다. `fault <초>`는 run.js가 부르는 장애 action이다.
import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { admin, compose, docker, ended, launch, oracle, ownedProject, sleep, until } from './stack.js'

const project = ownedProject()

if (process.argv[2] === 'fault') {
  // 체결 partition의 leader를 끊는다. AWS의 AZ 차단에서 feed가 재시작한 경우와 같은 조건이다.
  const describe = compose(['exec', '-T', 'kafka1', '/opt/kafka/bin/kafka-topics.sh', ...admin, '--describe', '--topic', 'wts.events.v1'])
  const broker = `kafka${describe.match(/Leader: (\d)/)[1]}`
  const container = compose(['ps', '-q', broker]).trim()
  const network = `${project}_default`
  const attached = JSON.parse(docker(['inspect', '--format', '{{json .NetworkSettings.Networks}}', container]))[network]
  let restored = false
  const restore = () => {
    if (restored) return
    restored = true
    try {
      // 같은 IP로 복원해 다른 broker의 DNS cache가 다른 인증서를 가진 피어를 가리키지 않게 한다.
      docker(['network', 'connect', '--alias', broker, '--ip', attached.IPAddress, network, container])
    } catch {
      // Docker 20.10은 Compose 기본 network에 IP 지정을 거절한다. 그때는 alias만 복원한다.
      docker(['network', 'connect', '--alias', broker, network, container])
    }
  }
  // 차단 중에 중단되어도 broker를 끊긴 채로 남기지 않는다.
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => { restore(); process.exit(1) })
  const started = performance.now()
  docker(['network', 'disconnect', network, container])
  try {
    await sleep(Number(process.argv[3]) * 1000)
  } finally {
    restore()
  }
  console.log(JSON.stringify({ ok: true, broker, isolated_ms: Math.round(performance.now() - started) }))
  process.exit(0)
}

const stack = await launch({ gateways: 2 })
try {
  const { feeds, gateways, temp } = stack
  const settings = {
    clients: 20, workers: 2, durationSeconds: 70, startupIntervalMs: 20, requireReconnectSlo: false,
    urls: stack.urls, tokenFile: stack.tokenFile,
    stateDirectory: join(temp, 'clients'), output: join(temp, 'report.json'), headCommand: oracle('head'), rangeCommand: oracle('range'),
    environment: { kind: 'single-host-compose', physical_domains_verified: false, external_lb_verified: false },
    actions: [{ name: 'isolate-events-leader', atSeconds: 15, command: [process.execPath, fileURLToPath(import.meta.url), 'fault', '25'] }],
  }
  writeFileSync(join(temp, 'load.json'), JSON.stringify(settings), { mode: 0o600 })
  const load = stack.start(process.execPath, ['bench/ha/run.js', join(temp, 'load.json')])
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
  await stack.stop()
}

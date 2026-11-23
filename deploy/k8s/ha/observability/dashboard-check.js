import { readFileSync, chmodSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFileSync } from 'node:child_process'
import assert from 'node:assert/strict'

const dashboard = JSON.parse(readFileSync(new URL('./dashboards/wts-overview.json', import.meta.url)))
const directory = mkdtempSync(join(tmpdir(), 'wts-dashboard-'))
chmodSync(directory, 0o755)
const expand = expr => expr.replaceAll('${namespace:regex}', 'test-a').replaceAll('${pod:regex}', '.*').replaceAll('$__rate_interval', '1m').replaceAll('${target:regex}', 'ha-wss')
const probeDashboard = JSON.parse(readFileSync(new URL('./dashboards/wts-probe.json', import.meta.url)))
const expressions = [...dashboard.panels, ...probeDashboard.panels].flatMap(panel => panel.targets ?? []).filter(target => target.datasource.type === 'prometheus').map(target => expand(target.expr))
try {
  writeFileSync(join(directory, 'rules.json'), JSON.stringify({ groups: [{ name: 'dashboards', rules: expressions.map((expr, i) => ({ record: `panel_${i}`, expr })) }] }))
  const expression = title => expand(dashboard.panels.find(panel => panel.title === title).targets.find(target => target.datasource.type === 'prometheus').expr)
  const series = (name, namespace, values) => ({ series: `${name}{namespace="${namespace}",pod="gateway"}`, values })
  const checks = [
    { expr: expression('Ready gateway 수'), eval_time: '1m', exp_samples: [{ labels: '{namespace="test-a"}', value: 1 }] },
    { expr: expression('큐 대기 p99'), eval_time: '1m', exp_samples: [] },
    { expr: expression('소켓 쓰기 p99'), eval_time: '1m', exp_samples: [{ labels: '{namespace="test-a",pod="gateway"}', value: 0.99 }] },
    { expr: expression('Kafka 위치 lag'), eval_time: '1m', exp_samples: [{ labels: 'wts_ha_log_position_lag{namespace="test-a",pod="gateway"}', value: 123 }] },
    { expr: expression('STOMP 활성 연결'), eval_time: '1m', exp_samples: [] },
  ]
  writeFileSync(join(directory, 'tests.json'), JSON.stringify({ rule_files: ['rules.json'], evaluation_interval: '5s', tests: [{ interval: '5s', input_series: [
    series('wts_ha_gateway_ready', 'test-a', '1x12'), series('wts_ha_gateway_ready', 'test-b', '9x12'),
    series('wts_ha_log_position_lag', 'test-a', '123x12'),
    series('wts_queue_wait_seconds_count', 'test-a', '0x12'),
    ...['1', '+Inf'].map(le => ({ series: `wts_queue_wait_seconds_bucket{namespace="test-a",pod="gateway",le="${le}"}`, values: '0x12' })),
    series('wts_socket_write_seconds_count', 'test-a', '100+1x5 0+1x6'),
    ...['1', '+Inf'].map(le => ({ series: `wts_socket_write_seconds_bucket{namespace="test-a",pod="gateway",le="${le}"}`, values: '100+1x5 0+1x6' })),
  ], promql_expr_test: checks }] }))
  execFileSync('docker', ['run', '--rm', '--entrypoint', '/bin/promtool', '-v', `${directory}:/rules:ro`, '-w', '/rules', 'prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996', 'test', 'rules', 'tests.json'], { stdio: 'inherit' })
  const ids = dashboard.panels.map(panel => panel.id)
  assert.equal(new Set(ids).size, ids.length)
  for (const panel of dashboard.panels.filter(panel => panel.type !== 'row')) {
    assert.equal(panel.fieldConfig.defaults.noValue, '측정되지 않음')
    for (const target of panel.targets) assert.doesNotMatch(target.expr, /or\s+vector\(0\)/)
  }
} finally { rmSync(directory, { recursive: true, force: true }) }

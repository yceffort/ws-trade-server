import { execFileSync } from 'node:child_process'
try {
  const [context, namespace] = process.argv.slice(2)
  if (!context || !namespace || !process.env.WTS_ROLLOUT_REQUEST) throw new Error('arguments required')
  const base = ['--context', context, '--namespace', namespace, '--request-timeout=10s']
  const pods = JSON.parse(
    execFileSync('kubectl', [...base, 'get', 'pods', '-l', 'app.kubernetes.io/component=ha-load', '-o', 'json'], {
      encoding: 'utf8',
      timeout: 15000
    })
  ).items.filter(p => p.status.phase === 'Running' && !p.metadata.deletionTimestamp)
  if (pods.length !== 1) throw new Error('one active HA load job required')
  const result = execFileSync(
    'kubectl',
    [
      ...base,
      'exec',
      pods[0].metadata.name,
      '-c',
      'load',
      '--',
      'env',
      `WTS_ROLLOUT_REQUEST=${process.env.WTS_ROLLOUT_REQUEST}`,
      'WTS_HA_CONTROL_SOCKET=/state/control.sock',
      'node',
      '/app/bench/ha/rollout-verify.js'
    ],
    { encoding: 'utf8', timeout: 95000, maxBuffer: 65536 }
  )
  const report = JSON.parse(result)
  if (report.ok !== true) throw new Error('oracle failed')
  console.log(JSON.stringify(report))
} catch {
  console.error('active HA load verification failed')
  process.exitCode = 1
}

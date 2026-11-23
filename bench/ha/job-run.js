import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs'
import { resolve, join } from 'node:path'
import { loadJob } from './job.js'
const exec = promisify(execFile),
  delay = ms => new Promise(resolve => setTimeout(resolve, ms))
try {
  const [context, settingsFile, optionsFile, directory] = process.argv.slice(2)
  if (!context || !directory) throw new Error('explicit context, settings, options and output directory required')
  const settings = JSON.parse(readFileSync(settingsFile)),
    options = JSON.parse(readFileSync(optionsFile))
  const manifest = loadJob(settings, options),
    output = resolve(directory)
  mkdirSync(output, { recursive: true, mode: 0o700 })
  const base = ['--context', context, '--namespace', settings.namespace, '--request-timeout=10s']
  const kubectl = async args =>
    (await exec('kubectl', [...base, ...args], { timeout: 15000, maxBuffer: 4 * 1024 * 1024 })).stdout
  const save = (name, text) => writeFileSync(join(output, name), text, { mode: 0o600 })
  const existing = await kubectl(['get', 'job', 'wts-ha-load', '--ignore-not-found', '-o', 'json'])
  if (existing.trim())
    throw new Error('previous wts-ha-load Job exists; archive its results and remove it before another run')
  save('load-job.json', JSON.stringify(manifest, null, 2))
  await kubectl(['apply', '--dry-run=server', '-f', join(output, 'load-job.json')])
  const collect = async () => {
    const pods = JSON.parse(
      await kubectl(['get', 'pods', '-l', 'app.kubernetes.io/part-of=wts-ha', '-o', 'json'])
    ).items
    save(
      'pods.json',
      JSON.stringify(
        pods.map(p => ({
          name: p.metadata.name,
          uid: p.metadata.uid,
          node: p.spec.nodeName,
          phase: p.status.phase,
          containers: [...(p.status.initContainerStatuses ?? []), ...(p.status.containerStatuses ?? [])].map(c => ({
            name: c.name,
            restarts: c.restartCount,
            state: c.state,
            lastState: c.lastState
          }))
        })),
        null,
        2
      )
    )
    for (const pod of pods.filter(
      p =>
        p.metadata.labels['app.kubernetes.io/component'] === 'gateway' &&
        p.status.phase === 'Running' &&
        !p.metadata.deletionTimestamp
    )) {
      try {
        save(
          `${pod.metadata.name}.prom`,
          await kubectl([
            'exec',
            pod.metadata.name,
            '-c',
            'gateway',
            '--',
            'curl',
            '--fail',
            '--silent',
            '--max-time',
            '2',
            'http://127.0.0.1:8081/ha-metrics'
          ])
        )
      } catch {
        save(`${pod.metadata.name}.missing`, 'metrics unavailable\n')
      }
    }
  }
  let report
  try {
    await kubectl(['apply', '-f', join(output, 'load-job.json')])
    const deadline = Date.now() + (manifest.items.find(o => o.kind === 'Job').spec.activeDeadlineSeconds + 15) * 1000
    let failure
    while (Date.now() < deadline) {
      let pods = []
      // 장애 시험 중에는 API 서버 호출이 잠깐 실패할 수 있다. 한 번의 실패로 실행 전체의 보고서를 잃지 않게 기한 안에서 다시 묻는다.
      try {
        pods = JSON.parse(
          await kubectl([
            'get',
            'pods',
            '-l',
            'app.kubernetes.io/part-of=wts-ha,app.kubernetes.io/component=ha-load',
            '-o',
            'json'
          ])
        ).items
        failure = undefined
      } catch (error) {
        failure = error
      }
      const pod = pods.find(p => p.status.phase === 'Running' && !p.metadata.deletionTimestamp)
      if (pod) {
        try {
          report = JSON.parse(
            await kubectl(['exec', pod.metadata.name, '-c', 'load', '--', 'cat', '/state/report.json'])
          )
        } catch {}
        if (report) break
      }
      if (pods.some(p => ['Failed', 'Succeeded'].includes(p.status.phase)))
        throw new Error('load pod exited without a collected report')
      await delay(5000)
    }
    if (!report) throw failure ?? new Error('load report deadline')
    save('report.json', JSON.stringify(report, null, 2))
  } finally {
    try {
      await collect()
    } catch {
      save('collection-error.txt', 'cluster diagnostics unavailable; inspect the dedicated namespace\n')
    }
  }
  console.log(JSON.stringify({ ok: report.ok, output, production_slo_proven: false }))
  if (report.ok !== true) process.exitCode = 1
} catch (error) {
  console.error(error.code ? 'HA load cluster operation failed' : error.message)
  process.exitCode = 1
}

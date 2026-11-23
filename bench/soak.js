#!/usr/bin/env node
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { writeFile } from 'node:fs/promises'
import { platform, arch, cpus } from 'node:os'
import { parseArgs } from 'node:util'
import { Environment } from './soak/environment.js'
import { scenario } from './soak/scenario.js'

const { values } = parseArgs({
  options: {
    seconds: { type: 'string', default: '600' },
    cycles: { type: 'string', default: '6' },
    connections: { type: 'string', default: '40' },
    'rss-growth-mib': { type: 'string', default: '64' },
    'bin-dir': { type: 'string', default: 'target/release' },
    out: { type: 'string' }
  }
})
const integer = (name, min, max) => {
  const number = Number(values[name])
  assert(Number.isInteger(number) && number >= min && number <= max, `${name} must be ${min}..${max}`)
  return number
}
const config = {
  seconds: integer('seconds', 1, 1800),
  cycles: integer('cycles', 1, 20),
  connections: integer('connections', 2, 200),
  rssGrowthMiB: integer('rss-growth-mib', 1, 1024),
  binDir: values['bin-dir']
}
assert(config.seconds >= config.cycles, 'seconds must be at least cycles')
const controller = new AbortController()
const abort = signal => controller.abort(new Error(signal))
process.once('SIGINT', () => abort('SIGINT'))
process.once('SIGTERM', () => abort('SIGTERM'))
const deadline = setTimeout(() => abort('scenario deadline'), (config.seconds + config.cycles * 15 + 60) * 1000)
const environment = new Environment(config, controller.signal)
const report = {
  config,
  startedAt: new Date().toISOString(),
  environment: {
    platform: platform(),
    arch: arch(),
    cpu: cpus()[0]?.model,
    node: process.version,
    commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim()
  }
}
try {
  await environment.start()
  await scenario(environment, config, report)
  assert.equal(environment.invalidLogs, 0, 'invalid JSON logs')
  report.ok = true
} catch (error) {
  report.ok = false
  report.error = controller.signal.aborted ? controller.signal.reason.message : error.message
  process.exitCode = 1
} finally {
  clearTimeout(deadline)
  try {
    await environment.close()
    if (report.ok) {
      assert.equal(environment.invalidLogs, 0, 'invalid JSON logs during shutdown')
      assert.equal(environment.events.shutdown_complete, 1, 'missing graceful shutdown log')
      assert.equal(environment.gateway.exitCode, 0, 'gateway shutdown failed')
    }
  } catch (error) {
    report.ok = false
    report.cleanupError = error.message
    process.exitCode = 1
  }
  report.completedAt = new Date().toISOString()
  report.logEvents = environment.events
  report.invalidLogs = environment.invalidLogs
  const json = `${JSON.stringify(report, null, 2)}\n`
  if (values.out) await writeFile(values.out, json)
  else process.stdout.write(json)
}

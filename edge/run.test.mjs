import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn, execFile } from 'node:child_process'
import { once } from 'node:events'
import { createInterface } from 'node:readline'
import { promisify } from 'node:util'
import { fileURLToPath } from 'node:url'

for (const host of ['', '0.0.0.0', '::1', '::']) {
  test(`CLI binding and probe agree for host ${JSON.stringify(host)}`, { timeout: 10000 }, async (t) => {
    const env = { ...process.env, WTS_EDGE_HOST: host, WTS_EDGE_PORT: '0', WTS_TLS_CERT_FILE: '', WTS_TLS_KEY_FILE: '' }
    const child = spawn(process.execPath, [fileURLToPath(new URL('run.mjs', import.meta.url))], { env, stdio: ['ignore', 'ignore', 'pipe'] })
    const exited = once(child, 'exit')
    const lines = createInterface({ input: child.stderr })
    t.after(async () => { child.kill('SIGTERM'); await exited; lines.close() })
    const [line] = await once(lines, 'line')
    const { level, target, fields } = JSON.parse(line)
    assert.deepEqual([level, target, fields.event], ['INFO', 'edge', 'edge_started'])
    assert(fields.url.startsWith(`ws://${host || '127.0.0.1'}:`), line)
    const port = fields.url.match(/:(\d+)\/ws$/)?.[1]
    assert(port, line)
    await promisify(execFile)(process.execPath, [fileURLToPath(new URL('tcp-probe.mjs', import.meta.url))], {
      env: { ...env, WTS_EDGE_PORT: port }, timeout: 3000,
    })
    child.kill('SIGTERM')
    assert.equal((await exited)[0], 0)
  })
}

import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as sleep } from 'node:timers/promises'
import { WebSocketServer } from '../edge/node_modules/ws/wrapper.mjs'
import { start } from './server.mjs'
import { render } from './render.mjs'

test('지속 실행기는 결과를 수집하고 종료할 때 worker와 타이머를 정리한다', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'wts-probe-'))
  const tokenFile = join(directory, 'token'); writeFileSync(tokenFile, 'synthetic')
  const mock = new WebSocketServer({ host: '127.0.0.1', port: 0, handleProtocols: () => 'v12.stomp' })
  await once(mock, 'listening')
  mock.on('connection', socket => socket.on('message', () => socket.send('ERROR\nmessage:auth-invalid\n\n\0')))
  const processHandle = start({ port: 0, intervalMs: 1000, targets: [{ target: 'local', protocol: 'stomp', url: `ws://127.0.0.1:${mock.address().port}/ws`, tokenFile, deadlineMs: 500 }] })
  try {
    const end = Date.now() + 5000
    while (processHandle.metrics.targets[0].last === null && Date.now() < end) await sleep(20)
    assert.notEqual(processHandle.metrics.targets[0].last, null)
    const response = await fetch(`http://127.0.0.1:${processHandle.server.address().port}/metrics`)
    const metrics = await response.text()
    assert.match(metrics, /wts_probe_success.* 0/)
    assert.match(metrics, /stage="auth"} 1/)
    assert.doesNotMatch(metrics, /synthetic|Bearer|cursor/)
  } finally {
    await processHandle.close(); for (const socket of mock.clients) socket.terminate(); mock.close()
    rmSync(directory, { recursive: true, force: true })
  }
})

test('Kubernetes probe는 관리 API 없이 metrics와 필요한 내부 경로만 연다', () => {
  const spec = render({ namespace: 'wts-ha', image: `local/probe@sha256:${'a'.repeat(64)}`, credentialsSecret: 'client', sourceConfigMap: 'source', config: { intervalMs: 30000, targets: [{ target: 'wss', protocol: 'stomp', url: 'wss://example.test/ws', tokenFile: '/credentials/token', deadlineMs: 1000 }] } })
  const pod = spec.items.find(item => item.kind === 'Deployment').spec.template.spec
  assert.equal(pod.automountServiceAccountToken, false)
  assert.equal(pod.containers[0].securityContext.readOnlyRootFilesystem, true)
  assert.equal(spec.items.find(item => item.kind === 'Service').spec.ports[0].port, 9464)
  assert.equal(spec.items.filter(item => item.kind === 'NetworkPolicy').length, 3)
})

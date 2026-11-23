import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { Client, topics, until } from '../bench/soak/client.js'

const [url = 'wss://127.0.0.1:8443/ws', directory = 'deploy/local', mode = 'receive'] = process.argv.slice(2)
assert(['receive', 'shutdown', 'reject'].includes(mode), 'unknown check mode')
const [{ token }] = JSON.parse(await readFile(join(directory, 'tokens.json'), 'utf8'))
const ca = await readFile(join(directory, 'cert.pem'))
const client = new Client(url, mode === 'reject' ? 'invalid-demo-token' : token, ca, AbortSignal.timeout(15000))
try {
  if (mode === 'reject') {
    await until(() => client.closed, 'invalid token rejection', client.signal)
    client.valid()
    assert.equal(client.serverError, 'auth-invalid')
  } else {
    await client.ready()
    assert.equal(client.ws.extensions, 'permessage-deflate')
    await until(
      () => {
        client.healthy()
        return topics.every(topic => client.topicMessages[topic] >= 3)
      },
      'both topics',
      client.signal
    )
    if (mode === 'shutdown') {
      console.log('ready-for-shutdown')
      await until(() => client.closed, 'server shutdown', client.signal, 12000)
      client.valid()
      assert.equal(client.serverError, 'server-shutdown')
    } else await client.close()
  }
  console.log(JSON.stringify({ ok: true, mode, messages: client.messages, topics: client.topicMessages }))
} finally {
  client.terminate()
}

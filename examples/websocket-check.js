import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { pathToFileURL } from 'node:url'
import { parseArgs } from 'node:util'
import { Client, topics, until } from '../bench/soak/client.js'

const contract = JSON.parse(await readFile(new URL('../contract/wts-stomp.json', import.meta.url), 'utf8'))

export function endpointUrl(value) {
  const endpoint = new URL(value)
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname)
  assert((endpoint.protocol === 'wss:' || endpoint.protocol === 'ws:' && loopback) && endpoint.pathname === '/ws')
  assert(!endpoint.username && !endpoint.password && !endpoint.search && !endpoint.hash)
  return endpoint.href
}

function observe(client) {
  const accepted = new Set()
  let unsubscribed = false
  client.ws.on('message', (bytes, binary) => {
    try {
      assert(!binary)
      const text = bytes.toString()
      if (text === '\n') return
      const split = text.indexOf('\n\n')
      const [command, ...lines] = text.slice(0, split).split('\n')
      const headers = Object.fromEntries(lines.map(line => {
        const colon = line.indexOf(':')
        return [line.slice(0, colon), line.slice(colon + 1)]
      }))
      if (command === 'RECEIPT') {
        assert.equal(headers['response-code'], '200')
        if (headers['receipt-id'] === 'unsubscribe-kr') {
          assert.equal(headers.message, 'unsubscribe success')
          unsubscribed = true
        } else {
          assert(topics.includes(headers['receipt-id']))
          assert.equal(headers.message, 'subscribe success')
          accepted.add(headers['receipt-id'])
        }
      }
      if (command === 'MESSAGE') {
        assert(accepted.has(headers.subscription), 'MESSAGE before RECEIPT')
        assert(!unsubscribed || headers.subscription !== topics[0], 'MESSAGE after unsubscribe RECEIPT')
        const market = headers.subscription === topics[0] ? 'kr' : 'us'
        const shape = contract.server.bodies[`/topic/v1/${market}/stock/trade/{code}`]
        const body = JSON.parse(text.slice(split + 2, -1))
        assert.deepEqual(Object.keys(body), shape.keyOrders[0])
        for (const [key, value] of Object.entries(body)) {
          const types = shape.fields[key].types
          assert(typeof value === 'string' ? types.includes('string') :
            typeof value === 'number' && Number.isFinite(value) &&
            (types.includes('decimal') || types.includes('integer') && Number.isInteger(value)))
        }
      }
    } catch {
      // 외부 서버가 보낸 원문이 오류 출력으로 새지 않도록 실패 사유를 고정한다.
      client.failure ??= 'invalid-frame-or-order'
    }
  })
  return client
}

export async function checkWebSocket({ url, token, ca, signal = AbortSignal.timeout(15000), onStep = () => {} }) {
  signal.throwIfAborted()
  url = endpointUrl(url)
  const result = { ok: false, protocol: 'v12.stomp', steps: [] }
  let client, phase = 'connect'
  const step = async (name, action) => {
    phase = name
    const started = performance.now()
    await action()
    const sample = { name, elapsed_ms: performance.now() - started }
    result.steps.push(sample)
    onStep(sample)
  }
  const open = async () => {
    client = observe(new Client(url, token, ca, signal))
    await client.ready()
    assert.equal(client.ws.protocol, 'v12.stomp')
  }
  const progress = async () => {
    const before = { ...client.topicMessages }
    await until(() => {
      client.healthy()
      return topics.every(topic => client.topicMessages[topic] >= before[topic] + 3)
    }, 'both topics progress', signal)
  }
  try {
    await step('connect-subscribe', open)
    await step('receive', progress)
    await step('unsubscribe', async () => {
      client.send('UNSUBSCRIBE', { id: topics[0], receipt: 'unsubscribe-kr' })
      await until(() => { client.healthy(); return client.receipts.has('unsubscribe-kr') }, 'unsubscribe receipt', signal)
      const before = { ...client.topicMessages }
      // 다른 구독이 계속 흐르는 동안 해지한 구독이 멈췄는지 확인한다.
      await until(() => { client.healthy(); return client.topicMessages[topics[1]] >= before[topics[1]] + 5 }, 'remaining subscription', signal)
      assert.equal(client.topicMessages[topics[0]], before[topics[0]])
    })
    const firstSession = { ...client.topicMessages }
    await step('disconnect', () => client.close())
    await step('reconnect-subscribe', open)
    await step('receive-after-reconnect', progress)
    const secondSession = { ...client.topicMessages }
    await step('close', () => client.close())
    return { ...result, ok: true, sessions: [firstSession, secondSession] }
  } catch {
    // 토큰, URL과 수신 본문 대신 실패한 단계만 결과에 남긴다.
    throw new Error(`websocket-check-failed:${phase}`)
  } finally {
    client?.terminate()
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const { values } = parseArgs({ options: {
      url: { type: 'string' }, 'token-file': { type: 'string' }, 'ca-file': { type: 'string' },
    } })
    const url = endpointUrl(values.url)
    const token = (await readFile(values['token-file'], 'utf8')).trim()
    assert(token && !/[\r\n\0]/.test(token))
    const ca = values['ca-file'] ? await readFile(values['ca-file']) : undefined
    const controller = new AbortController()
    for (const name of ['SIGINT', 'SIGTERM']) process.once(name, () => controller.abort())
    const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(15000)])
    const result = await checkWebSocket({ url, token, ca, signal })
    console.log(JSON.stringify(result, null, 2))
  } catch (error) {
    const code = error.message.startsWith('websocket-check-failed:') ? error.message : 'invalid-options-or-input'
    console.log(JSON.stringify({ ok: false, error: code }))
    process.exitCode = 1
  }
}

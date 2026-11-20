import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { createRequire } from 'node:module'
import { setTimeout as sleep } from 'node:timers/promises'

const WebSocket = createRequire(new URL('../../edge/package.json', import.meta.url))('ws')
export const topics = ['/topic/v1/kr/stock/trade/A005930', '/topic/v1/us/stock/trade/US19990122001']

export async function until(check, label, signal, millis = 5000) {
  const deadline = Date.now() + millis
  while (!await check()) {
    signal?.throwIfAborted()
    assert(Date.now() < deadline, `timeout: ${label}`)
    await sleep(20, undefined, { signal })
  }
}

export class Client {
  constructor(url, token, ca, signal) {
    this.id = `soak-${randomUUID()}`
    this.signal = signal
    this.messages = 0
    this.topicMessages = Object.fromEntries(topics.map((topic) => [topic, 0]))
    this.receipts = new Set()
    this.connected = false
    this.closed = false
    this.failure = null
    this.serverError = null
    this.ws = new WebSocket(url, 'v12.stomp', {
      ca, handshakeTimeout: 3000, maxPayload: 65536,
      perMessageDeflate: { clientNoContextTakeover: true, serverNoContextTakeover: true },
    })
    this.ws.on('error', () => { this.failure ??= 'websocket_error' })
    this.ws.on('close', () => { this.closed = true; clearInterval(this.heartbeat) })
    this.ws.on('message', (bytes, binary) => {
      try {
        assert(!binary, 'binary frame')
        const text = bytes.toString()
        if (text === '\n') return
        assert(text.endsWith('\0'), 'missing NUL')
        const split = text.indexOf('\n\n')
        assert(split >= 0, 'missing separator')
        const [command, ...lines] = text.slice(0, split).split('\n')
        const headers = Object.fromEntries(lines.map((line) => {
          const colon = line.indexOf(':'); return [line.slice(0, colon), line.slice(colon + 1)]
        }))
        if (command === 'CONNECTED') this.connected = true
        else if (command === 'RECEIPT') this.receipts.add(headers['receipt-id'])
        else if (command === 'ERROR') this.serverError = headers.message
        else if (command === 'MESSAGE') {
          const body = text.slice(split + 2, -1)
          assert.equal(headers['message-id'], `${this.id}-${this.messages + 1}`, 'message sequence')
          assert.equal(Number(headers['content-length']), Buffer.byteLength(body), 'content-length')
          assert(topics.includes(headers.subscription), 'unknown subscription')
          JSON.parse(body)
          this.messages++
          this.topicMessages[headers.subscription]++
        } else throw new Error('unexpected command')
      } catch (error) { this.failure ??= error.message }
    })
    this.ws.on('open', () => {
      this.send('CONNECT', { 'connection-id': this.id, authorization: `Bearer ${token}` })
      this.heartbeat = setInterval(() => {
        if (this.ws.readyState === WebSocket.OPEN) this.ws.send('\n')
      }, 250)
    })
  }

  send(command, headers) {
    assert.equal(this.ws.readyState, WebSocket.OPEN, 'socket must be open')
    this.ws.send(`${command}\n${Object.entries(headers).map(([key, value]) => `${key}:${value}`).join('\n')}\n\n\0`)
  }

  async ready() {
    await until(() => { this.healthy(); return this.connected }, 'CONNECTED', this.signal)
    for (const topic of topics) this.send('SUBSCRIBE', { id: topic, destination: topic, receipt: topic })
    await until(() => { this.healthy(); return topics.every((topic) => this.receipts.has(topic) && this.topicMessages[topic] > 0) }, 'subscriptions and data', this.signal)
    return this
  }

  valid() { assert(!this.failure, this.failure ?? 'client failed') }

  healthy() {
    this.valid()
    assert(!this.serverError, `server error: ${this.serverError}`)
    assert(!this.closed, 'unexpected disconnect')
  }

  async close() {
    this.healthy()
    this.ws.close(1000, 'soak')
    await until(() => this.closed, 'client close', this.signal)
    this.valid()
    assert(!this.serverError, `server error: ${this.serverError}`)
  }

  terminate() { clearInterval(this.heartbeat); this.ws.terminate() }
}

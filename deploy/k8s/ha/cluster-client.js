import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import WebSocket from '/app/node_modules/ws/wrapper.mjs'

const topics = ['/topic/v1/kr/stock/trade/A005930', '/topic/v1/us/stock/trade/US19990122001']
const urls = JSON.parse(process.env.WTS_URLS)
assert.ok(Array.isArray(urls) && urls.length >= 2)
for (const value of urls) {
  const url = new URL(value)
  assert.ok(
    url.protocol === 'wss:' &&
      url.pathname === '/experimental/ha/ws' &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash,
    'TLS HA endpoint required'
  )
}
const token = readFileSync('/client/token', 'utf8')
async function attempt(url, cursor) {
  const socket = new WebSocket(url, 'wts.ha.v1', { handshakeTimeout: 3000, servername: 'wts-gateway' })
  let saved = cursor,
    events = 0,
    first = null
  try {
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('cluster client deadline exceeded')), 20000)
      const done = (error, result) => {
        clearTimeout(timer)
        error ? reject(error) : resolve(result)
      }
      socket.on('error', () => done(new Error('cluster TLS/WebSocket connection failed')))
      socket.on('open', () => socket.send(JSON.stringify({ type: 'connect', token })))
      socket.on('message', bytes => {
        try {
          const frame = JSON.parse(bytes)
          if (frame.type === 'connected')
            socket.send(
              JSON.stringify({
                type: 'resume',
                request_id: 'cluster-smoke',
                topics,
                mode: cursor ? 'events' : 'state',
                ...(cursor ? { cursor } : {})
              })
            )
          if (frame.type === 'heartbeat') socket.send('{"type":"heartbeat"}')
          if (frame.type === 'error') throw new Error(`protocol error: ${frame.code}`)
          if (frame.type === 'event') {
            events++
            first ??= frame.position.offset
          }
          if (frame.type === 'checkpoint') {
            saved = frame.cursor
            if (events >= 5) done(null, { cursor: saved, events, first, offset: frame.offset })
          }
        } catch (error) {
          done(error)
        }
      })
    })
  } finally {
    socket.terminate()
  }
}
try {
  assert.ok(urls.length >= 2)
  const first = await attempt(urls[0], null)
  const second = await attempt(urls[1], first.cursor)
  assert.ok(BigInt(second.first) > BigInt(first.offset))
  console.log(
    JSON.stringify({
      ok: true,
      events: first.events + second.events,
      resumed_after_checkpoint: true,
      scope: 'TLS, shared auth and resume smoke; not a committed-event oracle or SLO result'
    })
  )
} catch (error) {
  console.error(error.message)
  process.exitCode = 1
}

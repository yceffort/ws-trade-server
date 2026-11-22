import { realpathSync } from 'node:fs'
import { createServer } from 'node:http'
import { pathToFileURL } from 'node:url'

export function receiver() {
  const events = []
  const server = createServer(async (request, response) => {
    try {
      if (request.method === 'GET' && request.url === '/notifications') {
        response.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(events))
        return
      }
      if (request.method !== 'POST' || request.url !== '/alerts') { response.writeHead(404).end(); return }
      const chunks = []; let size = 0
      for await (const chunk of request) {
        size += chunk.length
        if (size > 64 * 1024) { response.writeHead(413).end(); request.destroy(); return }
        chunks.push(chunk)
      }
      const body = JSON.parse(Buffer.concat(chunks))
      if (!['firing', 'resolved'].includes(body.status) || !Array.isArray(body.alerts) || body.alerts.length > 64) throw new Error('invalid notification')
      const safe = value => typeof value === 'string' && /^[a-zA-Z0-9_-]{1,128}$/.test(value) ? value : 'other'
      const event = { at: Date.now(), status: body.status, alerts: body.alerts.map(alert => ({ name: safe(alert.labels?.alertname), namespace: safe(alert.labels?.namespace), status: safe(alert.status) })) }
      events.push(event)
      if (events.length > 100) events.shift()
      console.log(JSON.stringify({ event: 'alert_notification', ...event }))
      response.writeHead(200).end()
    } catch { response.writeHead(400).end() }
  })
  server.maxConnections = 16
  server.requestTimeout = 5000; server.headersTimeout = 5000; server.keepAliveTimeout = 1000
  return server
}
if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  const server = receiver().listen(8080, '0.0.0.0')
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => { server.close(); server.closeAllConnections() })
}

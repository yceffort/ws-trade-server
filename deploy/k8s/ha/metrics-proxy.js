import { createServer, get } from 'node:http'
import { pathToFileURL } from 'node:url'
import { realpathSync } from 'node:fs'
export const isMain = (url, entry = process.argv[1]) =>
  Boolean(entry && url === pathToFileURL(realpathSync(entry)).href)
export function startMetrics({ backend = 'http://127.0.0.1:8081/ha-metrics', host = '0.0.0.0', port = 9090 } = {}) {
  const server = createServer((request, response) => {
    if (request.method !== 'GET' || request.url !== '/metrics') {
      response.writeHead(404).end()
      return
    }
    const upstream = get(backend, { timeout: 1000 }, result => {
      let text = ''
      result.on('data', chunk => {
        text += chunk
        if (text.length > 65536) upstream.destroy(new Error('metrics limit'))
      })
      result.on('end', () =>
        response
          .writeHead(result.statusCode === 200 ? 200 : 503, { 'content-type': 'text/plain; version=0.0.4' })
          .end(result.statusCode === 200 ? text : '')
      )
      result.on('error', () => response.destroy())
    })
    upstream.on('timeout', () => upstream.destroy(new Error('metrics deadline')))
    upstream.on('error', () => {
      if (!response.headersSent) response.writeHead(503).end()
      else response.destroy()
    })
    response.on('close', () => upstream.destroy())
  })
  server.maxConnections = 16
  server.headersTimeout = 3000
  server.requestTimeout = 3000
  server.keepAliveTimeout = 1000
  server.listen(port, host)
  return server
}
if (isMain(import.meta.url)) {
  const server = startMetrics()
  process.once('SIGTERM', () => {
    server.close()
    server.closeAllConnections()
  })
}

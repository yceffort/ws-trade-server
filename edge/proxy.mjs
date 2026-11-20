import http from 'node:http'
import https from 'node:https'
import { once } from 'node:events'
import WebSocket, { WebSocketServer } from 'ws'

export async function startProxy({ backend, tls, host = '127.0.0.1', port = 0, maxConnections = 1024, maxBuffer = 1 << 20, compression = false }) {
  const options = { headersTimeout: 5000, requestTimeout: 5000, maxHeaderSize: 8192 }
  const handler = (_, response) => { response.writeHead(404); response.end() }
  const server = tls ? https.createServer({ ...options, ...tls, minVersion: 'TLSv1.2', handshakeTimeout: 5000 }, handler) : http.createServer(options, handler)
  server.maxConnections = maxConnections
  const sockets = new Set()
  const backends = new Set()
  server.on('connection', (socket) => { sockets.add(socket); socket.on('error', () => {}); socket.once('close', () => sockets.delete(socket)) })
  const protocols = { '/ws': 'v12.stomp', '/experimental/ha/ws': 'wts.ha.v1' }
  const wss = new WebSocketServer({ noServer: true, perMessageDeflate: compression ? { serverNoContextTakeover: true, clientNoContextTakeover: true, concurrencyLimit: 4, threshold: 0 } : false, maxPayload: 65536, autoPong: false, handleProtocols: (_, request) => protocols[request.url] })
  server.on('upgrade', (request, socket, head) => {
    let rejected = false
    const reject = (status, retry) => { if (rejected) return; rejected = true; if (!socket.destroyed) socket.end(`HTTP/1.1 ${status}\r\n${retry ? `Retry-After: ${retry}\r\n` : ''}Connection: close\r\nContent-Length: 0\r\n\r\n`) }
    const protocol = Object.hasOwn(protocols, request.url) && protocols[request.url]
    if (!protocol) return reject('404 Not Found')
    if (!request.headers['sec-websocket-protocol']?.split(',').map((s) => s.trim()).includes(protocol)) return reject('400 Bad Request')
    socket.pause()
    const endpoint = new URL(backend)
    endpoint.pathname = request.url
    endpoint.search = ''
    const upstream = new WebSocket(endpoint, protocol, { perMessageDeflate: false, handshakeTimeout: 5000, maxPayload: 65536, autoPong: false })
    backends.add(upstream)
    let client
    const terminate = () => { client?.terminate(); upstream.terminate(); socket.destroy() }
    socket.once('close', () => { if (!client) upstream.terminate() })
    upstream.on('error', () => { if (client) terminate(); else reject('502 Bad Gateway') })
    upstream.once('unexpected-response', (_, response) => {
      const retry = Number(response.headers['retry-after'])
      if (response.statusCode === 503) reject('503 Service Unavailable', Number.isInteger(retry) && retry >= 1 && retry <= 60 ? retry : 1)
      else reject('502 Bad Gateway')
      response.destroy()
      upstream.terminate()
    })
    upstream.once('close', () => { backends.delete(upstream); if (!client && !rejected) socket.destroy() })
    upstream.once('open', () => {
      if (socket.destroyed) return upstream.terminate()
      try {
        wss.handleUpgrade(request, socket, head, (ws) => {
          client = ws
          const relay = (source, target) => {
            source.on('message', (data, binary) => {
              if (target.readyState !== WebSocket.OPEN) return
              if (target.bufferedAmount + data.length > maxBuffer) return terminate()
              target.send(data, { binary }, (error) => { if (error) terminate() })
            })
            for (const control of ['ping', 'pong']) source.on(control, (data) => {
              if (target.readyState !== WebSocket.OPEN) return
              if (target.bufferedAmount + data.length + 14 > maxBuffer) return terminate()
              target[control](data)
            })
            source.on('close', (code, reason) => {
              if (target.readyState === WebSocket.OPEN) target.close(code === 1005 || code === 1006 ? 1001 : code, reason)
            })
            source.on('error', terminate)
          }
          relay(client, upstream)
          relay(upstream, client)
          socket.resume()
        })
      } catch { terminate() }
    })
  })
  server.listen(port, host)
  await once(server, 'listening')
  return {
    address: server.address(),
    async close() {
      for (const socket of sockets) socket.destroy()
      for (const upstream of backends) upstream.terminate()
      await Promise.all([new Promise((resolve) => server.close(resolve)), new Promise((resolve) => wss.close(resolve))])
    },
  }
}

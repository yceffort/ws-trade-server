import http from 'node:http'
import https from 'node:https'
import { once } from 'node:events'
import WebSocket, { WebSocketServer } from 'ws'

// 연결을 거절하거나 끝낸 사유. 고정 목록만 세고 주소, 헤더, 토큰, 본문은 남기지 않는다.
export const REASONS = ['not_found', 'bad_protocol', 'upstream_unavailable', 'upstream_rejected', 'upstream_error', 'tls_aborted',
  'tls_handshake_failed', 'upgrade_failed', 'client_closed', 'upstream_closed', 'buffer_limit', 'relay_error', 'shutdown']

export async function startProxy({ backend, tls, host = '127.0.0.1', port = 0, maxConnections = 1024, maxBuffer = 1 << 20, compression = false }) {
  const counts = Object.fromEntries(REASONS.map((reason) => [reason, 0]))
  let closing = false
  const options = { headersTimeout: 5000, requestTimeout: 5000, maxHeaderSize: 8192 }
  const handler = (_, response) => { response.writeHead(404); response.end() }
  const server = tls ? https.createServer({ ...options, ...tls, minVersion: 'TLSv1.2', handshakeTimeout: 5000 }, handler) : http.createServer(options, handler)
  server.maxConnections = maxConnections
  // TCP probe처럼 TLS를 시작하지 않고 끊은 연결과 서버 인증서를 믿지 않아 끊은 클라이언트는 둘 다 ECONNRESET이라 가를 수 없다.
  server.on('tlsClientError', (error) => { counts[error.code === 'ECONNRESET' ? 'tls_aborted' : 'tls_handshake_failed']++ })
  const sockets = new Set()
  const backends = new Set()
  server.on('connection', (socket) => { sockets.add(socket); socket.on('error', () => {}); socket.once('close', () => sockets.delete(socket)) })
  const protocols = { '/ws': 'v12.stomp', '/experimental/ha/ws': 'wts.ha.v1' }
  const wss = new WebSocketServer({ noServer: true, perMessageDeflate: compression ? { serverNoContextTakeover: true, clientNoContextTakeover: true, concurrencyLimit: 4, threshold: 0 } : false, maxPayload: 65536, autoPong: false, handleProtocols: (_, request) => protocols[request.url] })
  server.on('upgrade', (request, socket, head) => {
    let rejected = false
    // upgrade 전의 연결도 하나는 한 사유로만 센다. 종료 중에는 shutdown으로 센다.
    const reject = (reason, status, retry) => { if (rejected) return; rejected = true; counts[closing ? 'shutdown' : reason]++; if (!socket.destroyed) socket.end(`HTTP/1.1 ${status}\r\n${retry ? `Retry-After: ${retry}\r\n` : ''}Connection: close\r\nContent-Length: 0\r\n\r\n`) }
    const protocol = Object.hasOwn(protocols, request.url) && protocols[request.url]
    if (!protocol) return reject('not_found', '404 Not Found')
    if (!request.headers['sec-websocket-protocol']?.split(',').map((s) => s.trim()).includes(protocol)) return reject('bad_protocol', '400 Bad Request')
    socket.pause()
    const endpoint = new URL(backend)
    endpoint.pathname = request.url
    endpoint.search = ''
    const upstream = new WebSocket(endpoint, protocol, { perMessageDeflate: false, handshakeTimeout: 5000, maxPayload: 65536, autoPong: false })
    backends.add(upstream)
    let client
    // 연결 하나는 처음 알게 된 사유 하나로만 센다. 종료 중에는 양쪽이 닫히는 것을 shutdown으로 센다.
    let ended = false
    const end = (reason) => { if (!ended) { ended = true; counts[closing ? 'shutdown' : reason]++ } }
    const terminate = (reason) => { end(reason); client?.terminate(); upstream.terminate(); socket.destroy() }
    // 클라이언트가 upgrade 전에 떠나거나 프록시가 닫으면 그 사유를 먼저 정한다. 뒤따르는 upstream 오류는 다시 세지 않는다.
    socket.once('close', () => { if (!client && !rejected) { rejected = true; counts[closing ? 'shutdown' : 'client_closed']++; upstream.terminate() } })
    upstream.on('error', () => { if (client) terminate('relay_error'); else reject('upstream_error', '502 Bad Gateway') })
    upstream.once('unexpected-response', (_, response) => {
      const retry = Number(response.headers['retry-after'])
      if (response.statusCode === 503) reject('upstream_unavailable', '503 Service Unavailable', Number.isInteger(retry) && retry >= 1 && retry <= 60 ? retry : 1)
      else reject('upstream_rejected', '502 Bad Gateway')
      response.destroy()
      upstream.terminate()
    })
    upstream.once('close', () => { backends.delete(upstream); if (!client && !rejected) { rejected = true; counts[closing ? 'shutdown' : 'upstream_error']++; socket.destroy() } })
    upstream.once('open', () => {
      if (socket.destroyed) return upstream.terminate()
      try {
        wss.handleUpgrade(request, socket, head, (ws) => {
          client = ws
          const relay = (source, target) => {
            const highWater = Math.min(maxBuffer, 64 * 1024)
            const sent = error => {
              if (error) return terminate('relay_error')
              if (target.bufferedAmount < highWater / 2 && source.readyState === WebSocket.OPEN && source.isPaused) source.resume()
            }
            source.on('message', (data, binary) => {
              if (target.readyState !== WebSocket.OPEN) return
              if (target.bufferedAmount + data.length > maxBuffer) return terminate('buffer_limit')
              target.send(data, { binary }, sent)
              if (target.bufferedAmount >= highWater) source.pause()
            })
            for (const control of ['ping', 'pong']) source.on(control, (data) => {
              if (target.readyState !== WebSocket.OPEN) return
              if (target.bufferedAmount + data.length + 14 > maxBuffer) return terminate('buffer_limit')
              target[control](data, undefined, sent)
            })
            source.on('close', (code, reason) => {
              end(source === client ? 'client_closed' : 'upstream_closed')
              if (target.readyState === WebSocket.OPEN) {
                target.resume()
                target.close(code === 1005 || code === 1006 ? 1001 : code, reason)
              }
            })
            source.on('error', () => terminate('relay_error'))
          }
          relay(client, upstream)
          relay(upstream, client)
          socket.resume()
        })
      } catch { terminate('upgrade_failed') }
    })
  })
  server.listen(port, host)
  await once(server, 'listening')
  return {
    address: server.address(),
    counts: () => ({ ...counts }),
    async close() {
      closing = true
      for (const socket of sockets) socket.destroy()
      for (const upstream of backends) upstream.terminate()
      await Promise.all([new Promise((resolve) => server.close(resolve)), new Promise((resolve) => wss.close(resolve))])
    },
  }
}

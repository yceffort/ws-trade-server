import net from 'node:net'

const configuredHost = process.env.WTS_EDGE_HOST || '127.0.0.1'
const host = configuredHost === '0.0.0.0' ? '127.0.0.1' : configuredHost === '::' ? '::1' : configuredHost
const socket = net.connect({ host, port: Number(process.env.WTS_EDGE_PORT ?? 8443) })
const fail = () => {
  socket.destroy()
  process.exitCode = 1
}
socket.setTimeout(2000, fail)
socket.once('error', fail)
socket.once('connect', () => socket.end())

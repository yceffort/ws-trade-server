import net from 'node:net'

const socket = net.connect({ host: '127.0.0.1', port: Number(process.env.WTS_EDGE_PORT ?? 8443) })
const fail = () => { socket.destroy(); process.exitCode = 1 }
socket.setTimeout(2000, fail)
socket.once('error', fail)
socket.once('connect', () => socket.end())

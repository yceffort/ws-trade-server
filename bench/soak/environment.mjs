import assert from 'node:assert/strict'
import { spawn, execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { mkdtemp, readFile, writeFile, rename, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { createInterface } from 'node:readline'
import { promisify } from 'node:util'
import { startProxy } from '../../edge/proxy.mjs'
import { Client, topics, until } from './client.mjs'

const exec = promisify(execFile)

export class Environment {
  constructor(config, signal) {
    this.config = config; this.signal = signal
    this.children = []; this.clients = new Set(); this.events = {}; this.invalidLogs = 0
    this.stable = randomUUID(); this.rotating = randomUUID()
  }

  async auth(includeRotating) {
    const tokens = [this.stable, ...(includeRotating ? [this.rotating] : [])]
    const expires_at = Math.floor(Date.now() / 1000) + 7200
    await writeFile(join(this.dir, 'tokens.new'), JSON.stringify(tokens.map((token) => ({ token, expires_at }))), { mode: 0o600 })
    await rename(join(this.dir, 'tokens.new'), join(this.dir, 'tokens.json'))
  }

  child(name, args, json = false) {
    const child = spawn(resolve(this.config.binDir, name), args, {
      stdio: ['ignore', 'ignore', 'pipe'], env: { ...process.env, WTS_LOG_FORMAT: 'json', RUST_LOG: 'ws_trade_server=info' },
    })
    child.once('error', () => { child.failed = true })
    child.finished = new Promise((done) => child.once('close', done))
    this.children.push(child)
    const lines = createInterface({ input: child.stderr })
    lines.on('line', (line) => {
      if (!json) return
      try {
        const row = JSON.parse(line); const event = row.fields?.event
        assert(event, 'missing event')
        this.events[event] = (this.events[event] ?? 0) + 1
        if (event === 'gateway_started') this.address = row.fields
      } catch { this.invalidLogs++ }
    })
    return child
  }

  feedStart() {
    this.feed = this.child('feed', ['--ring', join(this.dir, 'ring'), '--symbols', 'kr:A005930,us:US19990122001', '--rate', '50'])
  }

  async start() {
    this.dir = await mkdtemp(join(tmpdir(), 'ws-soak-'))
    await this.auth(true)
    await exec('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-noenc', '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1', '-keyout', join(this.dir, 'key.pem'), '-out', join(this.dir, 'cert.pem')], { timeout: 10000 })
    this.ca = await readFile(join(this.dir, 'cert.pem'))
    this.feedStart()
    this.gateway = this.child('gateway', ['--addr', '127.0.0.1:0', '--http-addr', '127.0.0.1:0', '--ring', join(this.dir, 'ring'), '--auth-file', join(this.dir, 'tokens.json'), '--snapshot-topics', topics.join(','), '--allowed-topics', topics.join(','), '--max-connections', String(this.config.connections + 4), '--max-subscriptions', '2', '--max-commands-per-sec', '20', '--idle-ms', '2000'], true)
    await until(() => { this.running(); return this.address }, 'gateway startup', this.signal)
    this.direct = `ws://${this.address.addr}/ws`
    assert.match(this.address.http_addr, /^Some\(.+\)$/)
    this.http = `http://${this.address.http_addr.slice(5, -1)}`
    this.proxy = await startProxy({ backend: this.direct, tls: { cert: this.ca, key: await readFile(join(this.dir, 'key.pem')) }, compression: true })
    this.url = `wss://127.0.0.1:${this.proxy.address.port}/ws`
  }

  running() {
    this.signal.throwIfAborted()
    for (const child of [this.gateway, this.feed].filter(Boolean)) {
      assert(!child.failed && child.exitCode === null && child.signalCode === null, 'child exited unexpectedly')
    }
  }

  async open(token = this.stable, direct = false) {
    const client = new Client(direct ? this.direct : this.url, token, this.ca, this.signal)
    this.clients.add(client)
    client.ws.once('close', () => this.clients.delete(client))
    try {
      await client.ready()
      if (!direct) assert.equal(client.ws.extensions, 'permessage-deflate')
      return client
    } catch (error) { client.terminate(); throw error }
  }

  async json(path) {
    this.running()
    const response = await fetch(`${this.http}/${path}`, { signal: AbortSignal.any([this.signal, AbortSignal.timeout(3000)]) })
    assert(response.ok, `HTTP ${response.status}`)
    return response.json()
  }

  async sample() {
    const metrics = await this.json('metrics')
    const { stdout } = await exec('ps', ['-o', 'rss=', '-p', String(this.gateway.pid)], { timeout: 3000 })
    const rssKiB = Number(stdout.trim())
    assert(Number.isFinite(rssKiB) && rssKiB > 0, 'invalid RSS sample')
    return { at: new Date().toISOString(), rssKiB, ...metrics }
  }

  async stop(child) {
    if (!child || child.exitCode !== null || child.signalCode !== null || child.failed) return
    child.kill('SIGTERM')
    const kill = setTimeout(() => child.kill('SIGKILL'), 5000)
    try { await child.finished } finally { clearTimeout(kill) }
  }

  async close() {
    for (const client of this.clients) client.terminate()
    try { await this.proxy?.close() } finally {
      await Promise.all(this.children.map((child) => this.stop(child)))
      if (this.dir) await rm(this.dir, { recursive: true, force: true })
    }
  }
}

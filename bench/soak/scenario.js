import assert from 'node:assert/strict'
import { setTimeout as sleep } from 'node:timers/promises'
import { topics, until } from './client.js'

export async function scenario(env, config, report) {
  const signal = env.signal
  const samples = report.samples = []
  const idle = report.idle = []
  const counters = report.counters = { sessions: 0, messages: 0, authRevocations: 0, reconnects: 0, rejectedStalls: 0, feedRestarts: 0 }
  const open = async (token) => { const client = await env.open(token); counters.sessions++; return client }
  const retire = async (client) => { await client.close(); counters.messages += client.messages }
  const sample = async () => {
    assert(samples.length < 3600, 'sample capacity exceeded')
    const point = await env.sample()
    for (const key of ['decode_failures_total', 'invalid_trades_total', 'ring_overruns_total', 'sequence_missing_total']) assert.equal(point[key], 0, key)
    samples.push(point)
    return point
  }
  const hold = async (clients, seconds) => {
    const untilTime = Date.now() + seconds * 1000
    while (Date.now() < untilTime) {
      clients.forEach((client) => client.healthy())
      const before = clients.map((client) => ({ ...client.topicMessages }))
      await sample()
      const wait = Math.min(1000, Math.max(0, untilTime - Date.now()))
      await sleep(wait, undefined, { signal })
      clients.forEach((client, index) => {
        client.healthy()
        if (wait >= 250) assert(topics.every((topic) => client.topicMessages[topic] > before[index][topic]), 'no topic progress during holding period')
      })
    }
  }
  for (let round = 0; round <= config.cycles; round++) {
    env.running()
    const clients = []
    for (let i = 0; i < config.connections - 1; i++) clients.push(await open())
    let rotating = await open(env.rotating)
    const duration = round === 0 ? 1 : config.seconds / config.cycles
    await hold([...clients, rotating], duration / 2)
    if (round > 0) {
      await env.auth(false)
      await until(() => rotating.closed, 'auth revocation', signal)
      rotating.valid()
      assert.equal(rotating.serverError, 'auth-revoked')
      counters.authRevocations++; counters.messages += rotating.messages
      clients.forEach((client) => client.healthy())
      await env.auth(true)
      await until(async () => {
        try { rotating = await open(env.rotating); return true }
        catch (error) { if (!error.message.includes('auth-invalid')) throw error; return false }
      }, 'auth restoration', signal)
      counters.reconnects++
      for (let i = 0; i < Math.ceil(clients.length / 2); i++) {
        await retire(clients[i]); clients[i] = await open(); counters.reconnects++
      }
      const snapshots = await env.json('snapshots')
      for (const topic of topics) {
        assert(snapshots[topic]?.body, 'missing snapshot')
        JSON.parse(snapshots[topic].body)
      }
      const stalled = await env.open(env.stable, true)
      counters.sessions++
      await until(async () => (await env.json('metrics')).stomp_connections === config.connections + 1, 'stalled admission', signal)
      stalled.ws.pause()
      stalled.send('SUBSCRIBE', { id: 'excess', destination: topics[0] })
      await until(async () => (await env.json('metrics')).stomp_connections === config.connections, 'stalled rejection cleanup', signal, 3000)
      stalled.ws.resume()
      await until(() => stalled.closed, 'stalled socket close', signal)
      stalled.valid()
      assert.equal(stalled.serverError, 'too-many-subscriptions')
      counters.rejectedStalls++; counters.messages += stalled.messages
      if (round === Math.ceil(config.cycles / 2)) {
        const before = (await env.json('metrics')).sequence_resets_total
        await env.stop(env.feed)
        env.feedStart()
        await until(async () => (await env.json('metrics')).sequence_resets_total > before, 'feed restart detection', signal)
        const resumed = [...clients, rotating].map((client) => ({ client, before: { ...client.topicMessages } }))
        await until(() => resumed.every(({ client, before }) => {
          client.healthy()
          return topics.every((topic) => client.topicMessages[topic] > before[topic])
        }), 'existing subscriptions after feed restart', signal)
        counters.feedRestarts++
      }
    }
    await hold([...clients, rotating], duration / 2)
    for (const client of [...clients, rotating]) await retire(client)
    await until(async () => {
      const m = await env.json('metrics')
      return m.stomp_connections === 0 && m.subscriptions === 0 && m.queued_frames === 0
    }, 'idle resources', signal)
    const point = await sample()
    idle.push({ round, rssKiB: point.rssKiB, connections: point.stomp_connections, subscriptions: point.subscriptions, queuedFrames: point.queued_frames })
    assert(point.rssKiB - idle[0].rssKiB <= config.rssGrowthMiB * 1024, 'idle RSS growth exceeds configured budget')
    process.stderr.write(`soak round ${round}/${config.cycles}: messages=${counters.messages}, idle_rss_kib=${point.rssKiB}\n`)
  }
  assert(counters.messages > 0 && counters.authRevocations === config.cycles && counters.rejectedStalls === config.cycles)
  report.rss = { baselineKiB: idle[0].rssKiB, finalKiB: idle.at(-1).rssKiB, peakKiB: Math.max(...samples.map((s) => s.rssKiB)), allowedGrowthMiB: config.rssGrowthMiB }
}

import test from 'node:test'
import assert from 'node:assert/strict'
import { LatestState } from './latest-state.js'
import { recover } from './recover.js'

test('retry captures a fresh stream version and restores quiet data', async () => {
  const state = new LatestState(),
    abort = new AbortController()
  let calls = 0
  const fetcher = async () => {
    if (++calls === 1) {
      state.stream('a', 'intermediate')
      return { ok: false, status: 503 }
    }
    return { ok: true, json: async () => ({ a: { body: 'latest' }, quiet: { body: 'last' } }) }
  }
  assert.equal(await recover(state, 'demo', abort.signal, { fetcher, retryMs: 1 }), true)
  assert.equal(calls, 2)
  assert.equal(state.values.get('a'), 'latest')
  assert.equal(state.values.get('quiet'), 'last')
})

test('disconnect cancels pending retries', async () => {
  const abort = new AbortController()
  let calls = 0
  const fetcher = async () => {
    calls++
    abort.abort()
    throw new Error('network')
  }
  assert.equal(await recover(new LatestState(), 'demo', abort.signal, { fetcher, retryMs: 1 }), false)
  assert.equal(calls, 1)
})

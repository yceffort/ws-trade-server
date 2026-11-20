import test from 'node:test'
import assert from 'node:assert/strict'
import { LatestState } from './latest-state.mjs'
test('snapshot restores quiet topics without replacing a newer stream value', () => {
  const state = new LatestState()
  const before = state.begin()
  state.stream('busy', 'new')
  state.snapshot({ busy: { body: 'old' }, quiet: { body: 'last' } }, before)
  assert.equal(state.values.get('busy'), 'new')
  assert.equal(state.values.get('quiet'), 'last')
})

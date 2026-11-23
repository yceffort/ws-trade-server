import test from 'node:test'
import assert from 'node:assert/strict'
import { experiment, loadSample } from './observability.js'

test('실험 로그에는 종류와 단계만 남기고 입력 식별자는 옮기지 않는다', () => {
  assert.deepEqual(experiment({ name: 'private-input', fault: 'private-id' }, 'start'), { level: 'INFO', event: 'ha_experiment', kind: 'fault', phase: 'start' })
  assert.equal(experiment({ window: 'private-id', rollback: true }, 'start').kind, 'rollback')
  assert.equal(experiment({ window: 'private-id' }, 'finish').kind, 'rollout')
  assert.equal(experiment(null, 'finish'), null)
})

test('event-loop 밀리초를 초로 바꾸고 표본 없음을 보존한다', () => {
  assert.equal(loadSample({ client_event_loop_p99_ms: 25 }).event_loop_p99_seconds, 0.025)
  for (const value of [null, undefined, NaN]) assert.equal(loadSample({ client_event_loop_p99_ms: value }).event_loop_p99_seconds, null)
  assert.deepEqual(Object.keys(loadSample({ client_event_loop_p99_ms: 25, token: 'private' })), ['level', 'event', 'event_loop_p99_seconds'])
})

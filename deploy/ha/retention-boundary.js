// 보관 경계 안의 cursor는 이어서 복구되고, 경계 밖의 cursor는 latest나 snapshot으로 옮겨 가지 않고 끝나는지 본다(#98).
// 시간과 용량 retention은 broker가 로그 시작 위치를 앞당기는 일이므로, 같은 결과를 DeleteRecords로 바로 만든다.
// smoke.sh가 만든 Compose 프로젝트에서 실행한다. 체결 로그의 앞부분을 지우므로 그 데이터셋의 마지막 시험으로 둔다.
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { runClient } from '../../examples/ha-client.js'
import { MemoryStore } from '../../examples/ha-state.js'
import { admin, compose, launch, oracle, ownedProject } from './stack.js'

ownedProject()
const command = ([file, ...args], extra = []) => {
  try { return { code: 0, out: JSON.parse(execFileSync(file, [...args, ...extra], { encoding: 'utf8' })) } }
  catch (error) { return { code: error.status, out: error.stdout ? JSON.parse(error.stdout) : null } }
}
const stack = await launch({ gateways: 1 })
try {
  const head = command(oracle('head')).out
  const startAt = { log_id: head.log_id, partition: 0, offset: head.next }
  const client = (store, extra = {}) => runClient({ urls: stack.urls, token: () => stack.token, store, startAt, ...extra })
  // 목표 위치 이상의 checkpoint를 저장하면 멈춘다. 그 시점의 상태와 cursor가 store에 남는다.
  const advance = async (store, target) => {
    const controller = new AbortController()
    // 진행하지 못하면 runClient가 계속 재시도하므로 기한을 두고, 기한으로 끝났으면 목표에 닿았는지 따로 확인한다.
    await client(store, { signal: AbortSignal.any([controller.signal, AbortSignal.timeout(60000)]), onCommit(state) { if (BigInt(state.offset) >= target) controller.abort() } })
    assert.ok(store.load().offset !== null && BigInt(store.load().offset) >= target, `client did not reach offset ${target}`)
    return store
  }
  const old = await advance(new MemoryStore('events', head.topics), BigInt(head.next) + 200n)
  const state = await advance(new MemoryStore('state', head.topics), BigInt(old.load().offset) + 1n)
  const kept = await advance(new MemoryStore('events', head.topics), BigInt(state.load().offset) + 200n)
  // cursor는 적용한 위치를 가리키고 재개는 그다음부터다. 경계를 kept 바로 다음으로 두면 kept만 보관 범위 안에 남는다.
  const boundary = BigInt(kept.load().offset) + 1n
  const request = JSON.stringify({ version: 1, partitions: [{ topic: 'wts.events.v1', partition: 0, offset: Number(boundary) }] })
  compose(['exec', '-T', 'kafka1', 'sh', '-c', `printf '%s' '${request}' > /tmp/delete-records.json && /opt/kafka/bin/kafka-delete-records.sh ${admin.join(' ')} --offset-json-file /tmp/delete-records.json >/dev/null`])
  const retention = command(oracle('retention'), ['86400'])
  assert.equal(retention.code, 2, 'retention below the target must exit with 2')
  assert.equal(retention.out.start, boundary.toString())
  assert.equal(retention.out.truncated, true)
  for (const store of [old, state]) {
    const before = store.load()
    // 서버가 만료된 cursor를 받아 주면 끝나지 않으므로 기한을 두고, 기한 안에 거절되어야 한다.
    await assert.rejects(client(store, { maxAttempts: 1, signal: AbortSignal.timeout(15000) }), /cursor-expired/)
    assert.deepEqual(store.load(), before, 'an expired cursor must not change the saved state')
  }
  await assert.rejects(client(new MemoryStore('events', head.topics), { maxAttempts: 1, signal: AbortSignal.timeout(15000) }), /start-out-of-range/)
  await advance(kept, boundary + 100n)
  console.log(JSON.stringify({ result: 'pass', boundary: boundary.toString(), expired_cursors: 2, resumed_at_boundary: true, retention: retention.out }))
} finally {
  await stack.stop()
}

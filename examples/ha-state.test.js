import test from 'node:test'
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Application, FileStore, MemoryStore } from './ha-state.js'

const topics = ['/topic/v1/kr/stock/trade/DEMO']
const event = seq => ({
  type: 'event',
  event_id: `demo/trades/epoch-1/${seq}`,
  topic: topics[0],
  position: { log_id: 'test-log', partition: 0, offset: String(seq) },
  body: { price: seq }
})
const checkpoint = offset => ({ type: 'checkpoint', offset: String(offset), cursor: `test-cursor-${offset}` })

test('disconnect before commit replays once and persists state and cursor together', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ha-state-'))
  try {
    const path = join(directory, 'client.json')
    let store = new FileStore(path, 'events', topics)
    let app = new Application(store)
    app.receive(event(1))
    assert.equal(store.load().count, '0')
    app = new Application(store)
    app.receive(event(1))
    app.receive(event(1))
    assert.deepEqual(app.receive(checkpoint(1)), ['demo/trades/epoch-1/1'])
    store = new FileStore(path, 'events', topics)
    assert.equal(store.load().count, '1')
    assert.equal(store.load().cursor, 'test-cursor-1')
    app = new Application(store)
    app.receive(event(1))
    app.receive(event(2))
    app.receive(checkpoint(2))
    assert.equal(store.load().count, '2')
    assert.equal(store.load().states[topics[0]].body.price, 2)
    assert.throws(() => new FileStore(path, 'state', topics), /scope/)
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test('snapshot chunks require checksum and a following checkpoint before commit', () => {
  const store = new MemoryStore('state', topics)
  let app = new Application(store)
  const bytes = Buffer.from(
    JSON.stringify({
      schema_version: 1,
      log_id: 'test-log',
      through: '3',
      topics: { [topics[0]]: null },
      padding: 'x'.repeat(50000)
    })
  )
  const chunks = Array.from({ length: Math.ceil(bytes.length / 32768) }, (_, i) =>
    bytes.subarray(i * 32768, (i + 1) * 32768)
  )
  const begin = { type: 'snapshot-begin', id: 'test', offset: '3', total_bytes: bytes.length, chunks: chunks.length }
  const end = {
    type: 'snapshot-end',
    id: 'test',
    offset: '3',
    total_bytes: bytes.length,
    sha256: createHash('sha256').update(bytes).digest('hex')
  }
  app.receive(begin)
  app.receive({ type: 'snapshot-chunk', id: 'test', index: 0, data: chunks[0].toString('base64') })
  assert.throws(() => app.receive(checkpoint(3)), /checkpoint/)
  assert.equal(store.load().cursor, null)
  app = new Application(store)
  app.receive(begin)
  chunks.forEach((chunk, index) =>
    app.receive({ type: 'snapshot-chunk', id: 'test', index, data: chunk.toString('base64') })
  )
  assert.throws(() => app.receive({ ...end, sha256: '0'.repeat(64) }), /checksum/)
  app.receive(end)
  assert.equal(store.load().cursor, null)
  app.receive(checkpoint(3))
  assert.equal(store.load().offset, '3')
  assert.equal(store.load().states[topics[0]], null)
})

test('numeric positions, unexpected sources and rewind checkpoints fail closed', () => {
  const app = new Application(new MemoryStore('events', topics))
  assert.throws(
    () => app.receive({ ...event(1), position: { log_id: 'test-log', partition: 0, offset: 1 } }),
    /position/
  )
  app.receive(event(1))
  app.receive(checkpoint(1))
  assert.throws(() => app.receive({ ...event(2), event_id: 'other/trades/epoch-1/2' }), /source/)
  assert.throws(() => app.receive(checkpoint(0)), /checkpoint/)
})

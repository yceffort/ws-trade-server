import { createHash, randomUUID } from 'node:crypto'
import { closeSync, constants, fstatSync, fsyncSync, openSync, readSync, renameSync, unlinkSync, writeFileSync } from 'node:fs'
import { dirname } from 'node:path'

const hash = value => createHash('sha256').update(value).digest('hex')
const number = value => {
  if (typeof value !== 'string' || !/^(0|[1-9]\d*)$/.test(value) || BigInt(value) > 9223372036854775807n) throw new Error('invalid-position')
  return BigInt(value)
}
const initial = (mode, topics) => ({ version: 1, mode, topics, cursor: null, offset: null, log_id: null, states: Object.fromEntries(topics.map(topic => [topic, null])), source: null, source_seq: null, count: '0', digest: '0'.repeat(64) })

export function readBounded(path, limit = 1024 * 1024) {
  const file = openSync(path, constants.O_RDONLY | constants.O_NONBLOCK)
  try {
    if (!fstatSync(file).isFile()) throw new Error('regular-file-required')
    const bytes = Buffer.alloc(limit + 1)
    let size = 0, count
    while (size < bytes.length && (count = readSync(file, bytes, size, bytes.length - size, null)) > 0) size += count
    if (size > limit) throw new Error('file-too-large')
    return bytes.subarray(0, size)
  } finally { closeSync(file) }
}

export class MemoryStore {
  constructor(mode, topics) { this.value = initial(mode, topics) }
  load() { return structuredClone(this.value) }
  commit(value) { this.value = structuredClone(value) }
}

export class FileStore extends MemoryStore {
  constructor(path, mode, topics) {
    super(mode, topics)
    this.path = path
    try {
      const data = readBounded(path)
      if (data.length > 1024 * 1024) throw new Error('state-too-large')
      this.value = JSON.parse(data)
      if (this.value.version !== 1 || this.value.mode !== mode || JSON.stringify(this.value.topics) !== JSON.stringify(topics)) throw new Error('state-scope-mismatch')
    } catch (error) { if (error.code !== 'ENOENT') throw error }
  }
  commit(value) {
    const bytes = Buffer.from(JSON.stringify(value))
    if (bytes.length > 1024 * 1024) throw new Error('state-too-large')
    const temporary = `${this.path}.${randomUUID()}.tmp`
    let fd
    try {
      fd = openSync(temporary, 'wx', 0o600)
      writeFileSync(fd, bytes)
      fsyncSync(fd)
      closeSync(fd); fd = undefined
      renameSync(temporary, this.path)
      const directory = openSync(dirname(this.path), 'r')
      try { fsyncSync(directory) } finally { closeSync(directory) }
      super.commit(value)
    } finally {
      if (fd !== undefined) closeSync(fd)
      try { unlinkSync(temporary) } catch (error) { if (error.code !== 'ENOENT') throw error }
    }
  }
}

export class Application {
  constructor(store) {
    this.store = store
    this.pending = store.load()
    this.snapshot = null
    this.snapshotComplete = this.pending.mode !== 'state' || Boolean(this.pending.cursor)
    this.applied = []
  }
  identity(id) {
    const match = /^([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)\/(0|[1-9]\d*)$/.exec(id)
    if (!match || BigInt(match[2]) > 18446744073709551615n) throw new Error('invalid-event-id')
    if (this.pending.source !== null && this.pending.source !== match[1]) throw new Error('source-changed')
    return { source: match[1], seq: BigInt(match[2]) }
  }
  receive(frame) {
    if (frame.type === 'snapshot-begin') {
      if (this.pending.mode !== 'state' || this.pending.cursor || this.snapshot || !Number.isInteger(frame.total_bytes) || frame.total_bytes < 1 || frame.total_bytes > 512 * 1024 || !Number.isInteger(frame.chunks) || frame.chunks !== Math.ceil(frame.total_bytes / 32768)) throw new Error('invalid-snapshot')
      number(frame.offset)
      this.snapshot = { ...frame, parts: [], bytes: 0 }
    } else if (frame.type === 'snapshot-chunk') {
      const snapshot = this.snapshot
      if (!snapshot || frame.id !== snapshot.id || frame.index !== snapshot.parts.length || frame.index >= snapshot.chunks || typeof frame.data !== 'string' || frame.data.length > 43692) throw new Error('invalid-snapshot')
      const bytes = Buffer.from(frame.data, 'base64')
      if (bytes.toString('base64') !== frame.data || bytes.length > 32768 || bytes.length === 0) throw new Error('invalid-snapshot')
      snapshot.bytes += bytes.length
      if (snapshot.bytes > snapshot.total_bytes) throw new Error('invalid-snapshot')
      snapshot.parts.push(bytes)
    } else if (frame.type === 'snapshot-end') {
      const snapshot = this.snapshot
      if (!snapshot || frame.id !== snapshot.id || frame.offset !== snapshot.offset || frame.total_bytes !== snapshot.total_bytes || snapshot.parts.length !== snapshot.chunks || snapshot.bytes !== snapshot.total_bytes) throw new Error('invalid-snapshot')
      const bytes = Buffer.concat(snapshot.parts)
      if (hash(bytes) !== frame.sha256) throw new Error('snapshot-checksum')
      const state = JSON.parse(bytes)
      if (state.schema_version !== 1 || state.through !== frame.offset || JSON.stringify(Object.keys(state.topics).sort()) !== JSON.stringify(this.pending.topics)) throw new Error('invalid-snapshot')
      this.pending.states = state.topics
      this.pending.log_id = state.log_id
      this.pending.offset = frame.offset
      for (const value of Object.values(state.topics)) {
        if (value === null) continue
        if (number(value.offset) > number(frame.offset)) throw new Error('invalid-snapshot')
        const { source, seq } = this.identity(value.event_id)
        this.pending.source = source
        if (this.pending.source_seq === null || seq > BigInt(this.pending.source_seq)) this.pending.source_seq = String(seq)
      }
      this.snapshot = null
      this.snapshotComplete = true
    } else if (frame.type === 'event') {
      if (this.snapshot || !this.pending.topics.includes(frame.topic) || frame.position?.partition !== 0) throw new Error('invalid-event')
      const offset = number(frame.position.offset)
      if (this.pending.log_id && this.pending.log_id !== frame.position.log_id) throw new Error('log-changed')
      const { source, seq } = this.identity(frame.event_id)
      const duplicate = this.pending.source_seq !== null && seq <= BigInt(this.pending.source_seq)
      if (!duplicate) {
        if (this.pending.offset !== null && offset <= number(this.pending.offset)) throw new Error('event-order')
        this.pending.states[frame.topic] = { event_id: frame.event_id, offset: frame.position.offset, body: frame.body }
        this.pending.source = source
        this.pending.source_seq = String(seq)
        this.pending.count = String(BigInt(this.pending.count) + 1n)
        this.pending.digest = hash(`${this.pending.digest}\n${frame.event_id}`)
        if (this.applied.length >= 10000) throw new Error('client-overflow')
        this.applied.push(frame.event_id)
      }
      this.pending.log_id = frame.position.log_id
      if (this.pending.offset === null || offset > number(this.pending.offset)) this.pending.offset = frame.position.offset
    } else if (frame.type === 'checkpoint') {
      if (this.snapshot || !this.snapshotComplete || typeof frame.cursor !== 'string' || !frame.cursor || frame.cursor.length > 4096 || this.pending.offset !== null && number(frame.offset) < number(this.pending.offset)) throw new Error('invalid-checkpoint')
      number(frame.offset)
      this.pending.offset = frame.offset
      this.pending.cursor = frame.cursor
      this.store.commit(this.pending)
      const applied = this.applied
      this.applied = []
      return applied
    }
    return []
  }
}

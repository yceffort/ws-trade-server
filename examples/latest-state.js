// 요청 중 새 스트림 값이 들어온 topic에는 그 요청의 스냅샷을 적용하지 않는다.
export class LatestState {
  values = new Map()
  versions = new Map()
  stream(topic, body) {
    this.versions.set(topic, (this.versions.get(topic) ?? 0) + 1)
    this.values.set(topic, body)
  }
  begin() { return new Map(this.versions) }
  snapshot(entries, before) {
    for (const [topic, entry] of Object.entries(entries)) {
      if (this.versions.get(topic) === before.get(topic)) this.values.set(topic, entry.body)
    }
  }
}

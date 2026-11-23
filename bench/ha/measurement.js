export class Measurement {
  constructor() {
    this.scope = null
    this.disconnected = null
  }
  mark(scope) {
    this.scope = scope
  }
  disconnect(at) {
    this.disconnected ??= { at, scope: this.scope }
  }
  caughtUp(at) {
    const previous = this.disconnected
    this.disconnected = null
    return {
      disconnect_ms: previous ? at - previous.at : null,
      window: previous?.scope?.window ?? null,
      fault: previous?.scope?.fault ?? null,
      scenario: previous?.scope?.name ?? null,
      fault_ms: previous?.scope?.name ? at - previous.scope.at : null
    }
  }
}
export function measuredTargets(report) {
  const feed = report.actions?.find(a => a.name === 'feed-failover' && a.result?.ok)?.result.feed_failover_ms
  const slots = rollback =>
    new Set(
      report.rollout_verifications
        ?.filter(v => v.ok && v.rollback === rollback && v.reconnect_slo_status === 'pass')
        .map(v => v.slot)
    ).size
  return (
    report.ok &&
    report.clients >= 1000 &&
    report.duration_ms >= 600000 &&
    report.reconnect_slo_status === 'pass' &&
    Number.isFinite(feed) &&
    feed >= 0 &&
    feed <= 30000 &&
    slots(false) === 3 &&
    slots(true) === 3
  )
}

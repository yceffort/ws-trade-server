export function experiment(scope, phase) {
  if (!scope) return null
  return { level: 'INFO', event: 'ha_experiment', kind: scope.name ? 'fault' : scope.rollback ? 'rollback' : 'rollout', phase }
}

export function loadSample(observed) {
  const seconds = observed.client_event_loop_p99_ms / 1000
  return { level: 'INFO', event: 'ha_load_sample', event_loop_p99_seconds: observed.client_event_loop_p99_ms !== null && Number.isFinite(seconds) ? seconds : null }
}

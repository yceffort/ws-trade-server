import { setTimeout as sleep } from 'node:timers/promises'

export async function recover(state, url, signal, { fetcher = fetch, retryMs = 500 } = {}) {
  while (!signal.aborted) {
    const before = state.begin()
    try {
      const response = await fetcher(url, { signal: AbortSignal.any([signal, AbortSignal.timeout(2000)]) })
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const entries = await response.json()
      if (signal.aborted) return false
      state.snapshot(entries, before)
      return true
    } catch {
      if (signal.aborted) return false
      await sleep(retryMs, undefined, { signal }).catch(() => {})
    }
  }
  return false
}

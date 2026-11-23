import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'

// 인증 자료 전체를 artifact로 남기지 않고 실패 원인에 필요한 오류 행만 출력한다.
let output
try {
  output = execFileSync('docker', ['compose', '-f', 'deploy/ha/compose.yaml', 'logs', '--no-color', '--tail', '200'], {
    encoding: 'utf8',
    maxBuffer: 4 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'pipe']
  })
} catch {
  console.error('broker 진단 로그를 읽지 못했습니다.')
  process.exit(0)
}
for (const role of ['admin', 'feed', 'gateway']) {
  try {
    const config = JSON.parse(readFileSync(`deploy/local/ha/${role}.json`, 'utf8'))
    for (const [key, value] of Object.entries(config.properties)) {
      if (/password|secret|token/i.test(key) && value) output = output.replaceAll(value, '[redacted]')
    }
  } catch {
    /* 준비 단계 자체가 실패했으면 인증 파일이 없을 수 있다. */
  }
}
output = output.replace(/-----BEGIN[^-]+-----[\s\S]*?-----END[^-]+-----/g, '[redacted-pem]')
const lines = output
  .split('\n')
  .filter(
    line =>
      /error|exception|permission denied|no such|invalid|failed|unable|accessdenied/i.test(line) &&
      !/jaas|password|secret|token|private.key/i.test(line)
  )
console.error(
  lines
    .slice(-30)
    .map(line => line.slice(0, 1000))
    .join('\n') || '오류 행이 없습니다.'
)

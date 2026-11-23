import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { createPrivateKey, sign } from 'node:crypto'
import { pathToFileURL } from 'node:url'
import { render } from './render.js'

// gateway는 한 시간보다 긴 token을 받지 않으므로, 몇 시간짜리 시험은 실행마다 새 token을 만들어 쓴다.
export function clientToken(privateKey, issuer, topics, now = Math.floor(Date.now() / 1000)) {
  const encoded = value => Buffer.from(JSON.stringify(value)).toString('base64url')
  const message = `${encoded({ alg: 'RS256', typ: 'at+jwt', kid: 'dev-1' })}.${encoded({ iss: issuer, aud: 'wts-ha', sub: 'dev-client', jti: 'dev-client-1', iat: now, nbf: now, exp: now + 3600, topics })}`
  return `Bearer ${message}.${sign('RSA-SHA256', Buffer.from(message), privateKey).toString('base64url')}`
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const settings = JSON.parse(readFileSync(process.argv[2]))
    const topics = JSON.parse(render(settings).items.find(o => o.kind === 'ConfigMap').data['source.json']).topics
    console.log(
      clientToken(
        createPrivateKey(readFileSync(join(process.argv[3], 'client-key.pem'))),
        `https://wts-policy.${settings.namespace}.svc`,
        topics
      )
    )
  } catch {
    console.error('settings와 dev-secrets.js의 출력 디렉터리가 필요합니다.')
    process.exitCode = 1
  }
}

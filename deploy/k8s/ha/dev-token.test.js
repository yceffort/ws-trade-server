import test from 'node:test'
import assert from 'node:assert/strict'
import { generateKeyPairSync, verify } from 'node:crypto'
import { clientToken } from './dev-token.js'

test('개발용 client token은 한 시간짜리이고 정책 공급자의 공개 키로 검증된다', () => {
  const { publicKey, privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 })
  const token = clientToken(privateKey, 'https://wts-policy.wts-ha.svc', ['a', 'b'], 1000)
  assert.ok(token.startsWith('Bearer '))
  const [header, claims, signature] = token.slice(7).split('.')
  assert.ok(verify('RSA-SHA256', Buffer.from(`${header}.${claims}`), publicKey, Buffer.from(signature, 'base64url')))
  assert.deepEqual(JSON.parse(Buffer.from(header, 'base64url')), { alg: 'RS256', typ: 'at+jwt', kid: 'dev-1' })
  assert.deepEqual(JSON.parse(Buffer.from(claims, 'base64url')), { iss: 'https://wts-policy.wts-ha.svc', aud: 'wts-ha', sub: 'dev-client', jti: 'dev-client-1', iat: 1000, nbf: 1000, exp: 4600, topics: ['a', 'b'] })
})

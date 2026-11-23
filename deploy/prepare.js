import { mkdir, writeFile, chmod } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { resolve, join } from 'node:path'

const directory = resolve(process.argv[2] ?? 'deploy/local')
// 기존 인증 자료를 덮어쓰지 않는다. 호스트에서는 이 디렉터리 소유자만 파일을 열 수 있다.
await mkdir(directory, { mode: 0o700 })
const cert = join(directory, 'cert.pem'),
  key = join(directory, 'key.pem')
const names = 'DNS:localhost,IP:127.0.0.1' + (process.argv[3] ? `,DNS:${process.argv[3]}` : '')
execFileSync(
  'openssl',
  [
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-noenc',
    '-days',
    '1',
    '-subj',
    '/CN=localhost',
    '-addext',
    `subjectAltName=${names}`,
    '-keyout',
    key,
    '-out',
    cert
  ],
  { stdio: 'ignore' }
)
await writeFile(
  join(directory, 'tokens.json'),
  JSON.stringify([{ token: randomUUID(), expires_at: Math.floor(Date.now() / 1000) + 86400 }]) + '\n'
)
// Compose의 파일 기반 secret은 UID 변환을 하지 않으므로 컨테이너의 비특권 UID에도 읽기를 허용한다.
for (const file of [cert, key, join(directory, 'tokens.json')]) await chmod(file, 0o444)
console.log('데모 인증 자료 생성: ' + directory)

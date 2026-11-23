import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, copyFile, readdir, rm } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const allowed = [
  'Cargo.toml',
  'Cargo.lock',
  'build.rs',
  'src/lib.rs',
  'proto/feed.proto',
  'deploy/container/probe.sh',
  'edge/package.json',
  'edge/package-lock.json',
  'edge/run.js',
  'edge/proxy.js',
  'edge/tcp-probe.js'
]
const excluded = [
  '.git/config',
  '.env',
  'README.md',
  'evidence/raw/capture.json',
  'deploy/local/tokens.json',
  'deploy/README.md',
  'edge/certs/key.pem',
  'edge/node_modules/ws/index.js',
  'edge/README.md'
]
const temporary = await mkdtemp(join(tmpdir(), 'wts-context-'))
try {
  const context = join(temporary, 'input'),
    output = join(temporary, 'output')
  for (const file of [...allowed, ...excluded]) {
    const path = join(context, file)
    await mkdir(join(path, '..'), { recursive: true })
    await writeFile(path, 'synthetic-build-context-fixture\n')
  }
  await copyFile(new URL('../.dockerignore', import.meta.url), join(context, '.dockerignore'))
  // 합성 파일만 있는 별도 context에서 COPY 전체의 결과를 검사한다.
  execFileSync('docker', ['build', '--quiet', '--file', '-', '--output', `type=local,dest=${output}`, context], {
    env: { ...process.env, BUILDX_GIT_INFO: 'false' },
    input: 'FROM scratch\nCOPY . /context\n',
    stdio: ['pipe', 'pipe', 'inherit']
  })
  async function files(directory, prefix = '') {
    const result = []
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name),
        name = prefix + entry.name
      result.push(...(entry.isDirectory() ? await files(path, name + '/') : [name]))
    }
    return result
  }
  assert.deepEqual((await files(join(output, 'context'))).sort(), allowed.sort())
  console.log('build context allowlist: passed')
} finally {
  await rm(temporary, { recursive: true, force: true })
}

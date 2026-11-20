import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
const worker = fileURLToPath(new URL('./bench-worker.mjs', import.meta.url))
const results = ['plain', 'deflate'].map((mode) => JSON.parse(execFileSync(process.execPath, [worker, mode, process.argv[2] ?? '1000'], { encoding: 'utf8', timeout: 120000 })))
if (results[0].dataset_sha256 !== results[1].dataset_sha256) throw new Error('비교 데이터가 다르다')
console.log(JSON.stringify({ scope: 'one isolated process per mode: proxy + echo backend + client', results }, null, 2))

import test from 'node:test'
import assert from 'node:assert/strict'
import { canonicalImage } from './kind-images.mjs'
test('Docker familiar image names resolve to containerd canonical names',()=>{
  for(const [input,expected] of [['gateway','docker.io/library/gateway:latest'],['gateway:dev','docker.io/library/gateway:dev'],['team/gateway:dev','docker.io/team/gateway:dev'],['localhost:5000/team/gateway','localhost:5000/team/gateway:latest'],['ghcr.io/team/gateway:dev','ghcr.io/team/gateway:dev'],['team/gateway@sha256:abcd','docker.io/team/gateway@sha256:abcd']]) assert.equal(canonicalImage(input),expected)
})

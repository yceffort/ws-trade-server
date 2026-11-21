import test from 'node:test'
import assert from 'node:assert/strict'
import { faultNode } from './kind-fault.mjs'
const pod=(node,phase='Running')=>({spec:{nodeName:node},status:{phase}})
test('node faults preserve the load Pod and its durable journal',()=>{
  const gateways=[pod('a'),pod('b'),pod('c')]
  assert.equal(faultNode(gateways,[pod('a')]),'b')
  assert.equal(faultNode(gateways,[pod('a'),pod('b','Pending')]),'c')
  assert.throws(()=>faultNode(gateways,[pod('a'),pod('b'),pod('c')]),/without a load Pod/)
})

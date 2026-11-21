import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,rmSync,appendFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Application } from '../../examples/ha-state.mjs'
import { Journal,recover } from './journal.mjs'
test('checkpoint states and cursors become observable together after group fsync',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'ha-journal-')),path=join(directory,'clients')
  try{
    const journal=new Journal(path),a=journal.store(1,['a']),b=journal.store(2,['b'])
    const application=new Application(a)
    application.receive({type:'event',event_id:'demo/trades/epoch/1',topic:'a',position:{log_id:'test',partition:0,offset:'10'},body:{price:42}})
    application.receive({type:'checkpoint',offset:'10',cursor:'cursor-a'})
    b.commit({...b.load(),offset:'20',cursor:'cursor-b',count:'2'})
    assert.equal(a.durable().cursor,null)
    await Promise.all([a.flush(),b.flush()])
    assert.equal(a.durable().cursor,'cursor-a');assert.equal(a.durable().states.a.body.price,42);assert.equal(b.durable().count,'2')
    await journal.close()
    appendFileSync(path,'{"id":1,"state":')
    const restored=recover(path)
    assert.deepEqual(restored.get(1),a.durable());assert.deepEqual(restored.get(2),b.durable())
  }finally{rmSync(directory,{recursive:true,force:true})}
})
test('journal compaction preserves the newest durable checkpoint and permits later appends',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'ha-journal-')),path=join(directory,'clients')
  try{
    const journal=new Journal(path),store=journal.store(1,['a'])
    for(let i=0;i<9;i++){
      store.commit({...store.load(),offset:String(i),cursor:`cursor-${i}`,padding:'x'.repeat(2*1024*1024)})
      await store.flush()
    }
    store.commit({...store.load(),offset:'10',cursor:'cursor-final',padding:''})
    await store.flush();await journal.close()
    assert.deepEqual(recover(path).get(1),store.durable())
    assert.equal(store.durable().cursor,'cursor-final')
  }finally{rmSync(directory,{recursive:true,force:true})}
})

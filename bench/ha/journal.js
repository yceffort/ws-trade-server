import { openSync,writeFileSync,fsyncSync,closeSync,renameSync,readFileSync,statSync } from 'node:fs'
import { dirname } from 'node:path'
import { MemoryStore } from '../../examples/ha-state.js'
const empty=topics=>new MemoryStore('events',[...topics].sort()).load()
export function recover(path) {
  if(statSync(path).size>32*1024*1024)throw new Error('client journal limit')
  const text=readFileSync(path,'utf8'),states=new Map()
  const end=text.lastIndexOf('\n')
  if(end<0)return states
  for(const line of text.slice(0,end).split('\n').filter(Boolean)){
    const {id,state}=JSON.parse(line)
    if(!Number.isInteger(id)||id<0||!state||state.mode!=='events'||typeof state.count!=='string'||!Array.isArray(state.topics))throw new Error('invalid client journal')
    states.set(id,state)
  }
  return states
}
export class Journal {
  constructor(path) {
    this.path=path;this.fd=openSync(path,'wx',0o600);this.directory=openSync(dirname(path),'r');fsyncSync(this.directory)
    this.states=new Map();this.pending=new Map();this.waiters=[];this.bytes=0;this.timer=null;this.failed=null
  }
  store(id,topics) {
    let current=empty(topics)
    return {
      load:()=>structuredClone(current),
      durable:()=>structuredClone(this.states.get(id)??empty(topics)),
      commit:state=>{current=structuredClone(state);this.pending.set(id,current)},
      flush:()=>this.flush()
    }
  }
  flush() {
    if(this.failed)return Promise.reject(this.failed)
    return new Promise((resolve,reject)=>{
      this.waiters.push({resolve,reject})
      this.timer??=setTimeout(()=>this.write(),10)
    })
  }
  write() {
    this.timer=null
    const waiters=this.waiters;this.waiters=[]
    try {
      const text=[...this.pending].map(([id,state])=>JSON.stringify({id,state})+'\n').join('')
      if(Buffer.byteLength(text)>4*1024*1024)throw new Error('client checkpoint batch limit')
      writeFileSync(this.fd,text);fsyncSync(this.fd);this.bytes+=Buffer.byteLength(text)
      for(const [id,state] of this.pending)this.states.set(id,state)
      this.pending.clear()
      if(this.bytes>16*1024*1024){
        const next=`${this.path}.next`,fd=openSync(next,'wx',0o600)
        try{writeFileSync(fd,[...this.states].map(([id,state])=>JSON.stringify({id,state})+'\n').join(''));fsyncSync(fd)}finally{closeSync(fd)}
        renameSync(next,this.path);fsyncSync(this.directory);closeSync(this.fd);this.fd=openSync(this.path,'a');this.bytes=statSync(this.path).size
      }
      for(const waiter of waiters)waiter.resolve()
    }catch(error){this.failed=error;for(const waiter of waiters)waiter.reject(error)}
  }
  async close(){if(this.pending.size)await this.flush();closeSync(this.fd);closeSync(this.directory)}
}

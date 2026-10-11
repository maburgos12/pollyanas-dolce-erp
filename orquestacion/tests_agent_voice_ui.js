const assert=require('node:assert/strict');
const {wav}=require('../static/js/erp-agent-voice.js');
async function run() {
  const blob=wav([new Float32Array([-1,-0.5,0,0.5,1])],16000);
  const buffer=await blob.arrayBuffer(); const data=new DataView(buffer);
  assert.equal(blob.type,'audio/wav'); assert.equal(buffer.byteLength,54);
  assert.equal(data.getUint32(24,true),16000); assert.equal(data.getUint16(22,true),1);
  assert.equal(data.getUint32(40,true),10);
  assert.deepEqual([0,1,2,3,4].map(i=>data.getInt16(44+2*i,true)),[-32768,-16384,0,16384,32767]);
  assert.throws(()=>wav([],16000)); assert.throws(()=>wav([new Float32Array(8000*180+1)],8000));
  console.log('Voice WAV checks passed: mono PCM16, rate, frames, clipping and duration bound.');
}
run().catch(error=>{console.error(error);process.exitCode=1;});

async function captureChecks() {
  const fs=require('node:fs'); const vm=require('node:vm');
  const elem=()=>({listeners:{},disabled:false,hidden:false,value:'',files:[],textContent:'',
    addEventListener(name,fn){this.listeners[name]=fn;},setAttribute(){},removeAttribute(){}});
  const button=elem(),input=elem(),preview=elem(),remove=elem(),status=elem(),document=elem();
  let tracksStopped=0,contextsClosed=0,processor;
  class Context {
    constructor(){this.sampleRate=16000;this.state='running';this.audioWorklet={addModule:async()=>{}};this.destination={};}
    async resume(){} async close(){this.state='closed';contextsClosed++;}
    createMediaStreamSource(){return {connect(){},disconnect(){}};}
    createGain(){return {gain:{value:1},connect(){}};}
  }
  class Processor {constructor(){processor=this;this.port={close(){}};}connect(){}disconnect(){}}
  const sandbox=vm.createContext({Blob,File,Float32Array,DataView,ArrayBuffer,AudioContext:Context,AudioWorkletNode:Processor,
    document,navigator:{mediaDevices:{getUserMedia:async()=>({getTracks:()=>[{stop(){tracksStopped++;}}]})}},
    URL:{createObjectURL:()=> 'blob:unit-test',revokeObjectURL(){}},setTimeout:()=>1,clearTimeout(){},addEventListener(){}});
  vm.runInContext(fs.readFileSync('static/js/erp-agent-voice.js','utf8'),sandbox);
  const states=[];
  const voice=sandbox.ERPVoice.attach({button,input,preview,remove,status,worklet:'unit-worklet',onRecording:value=>states.push(value)});
  voice.setBusy(false);assert.equal(remove.hidden,true);
  await button.listeners.click();assert.equal(button.textContent,'Detener grabación');
  processor.port.onmessage({data:new Float32Array(16000)});
  await button.listeners.click();assert(voice.file);assert.equal(voice.file.type,'audio/wav');
  assert.equal(remove.hidden,false);
  assert.equal(tracksStopped,1);assert.equal(contextsClosed,1);assert.equal(states.at(-1),false);
  voice.clear();assert.equal(voice.file,null);assert.equal(remove.hidden,true);
  sandbox.navigator.mediaDevices.getUserMedia=async()=>{const e=new Error('denied');e.name='NotAllowedError';throw e;};
  await button.listeners.click();assert(status.textContent.includes('no tiene permiso'));assert.equal(states.at(-1),false);
  assert.equal(contextsClosed,2);assert.equal(voice.file,null);
  console.log('Voice capture checks passed: native PCM, stop releases tracks, permission denial and preview cleanup.');
}
captureChecks().catch(error=>{console.error(error);process.exitCode=1;});

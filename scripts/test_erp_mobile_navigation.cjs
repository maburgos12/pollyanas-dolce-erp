// Run with node; these mocks exercise lifecycle logic without a phone or database.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = name => fs.readFileSync(path.join(__dirname, '..', 'static/js', name), 'utf8');
function mobile({url='https://erp.test/rrhh/gafetes/?page=2&q=private', user='7', standalone=true, saved, active='7', type='navigate', loading=false}={}) {
  const storage = new Map();
  if (saved) storage.set('erp_resume_'+active, JSON.stringify(saved));
  storage.set('erp_resume_active', active);
  const events = {}, main = {scrollTop:0}, sidebar = {scrollTop:0};
  const context = {URL, URLSearchParams, console, navigator:{standalone}, location:new URL(url),
    localStorage:{getItem:k=>storage.get(k)||null,setItem:(k,v)=>storage.set(k,v),removeItem:k=>storage.delete(k)},
    performance:{getEntriesByType:()=>[{type}]}, history:{replaceState(){}},
    document:{currentScript:{dataset:{user,method:'GET'}},cookie:'',hidden:false,
      querySelectorAll:()=>[{href:'https://erp.test/rrhh/gafetes/'}],
      querySelector:s=>s==='[data-dashboard-loading]'?(loading?{}:null):s==='.main-content'?main:sidebar,
      addEventListener:(n,f)=>events[n]=f},
    window:{matchMedia:()=>({matches:false}),scrollY:320,scrollTo:(x,y)=>context.window.scrollY=y,
      location:new URL(url),setTimeout:fn=>fn(),clearTimeout(){},addEventListener:(n,f)=>{const previous=events[n];events[n]=previous?()=>{previous();f()}:f}}};
  vm.runInNewContext(source('erp_mobile_navigation.js'), context);
  return {context,storage,events,main,sidebar};
}
let result=mobile();
assert.equal(JSON.parse(result.storage.get('erp_resume_7')).url, '/rrhh/gafetes/?page=2');
assert(!result.context.document.cookie.includes('private'));
result.context.window.scrollY=650; result.events.visibilitychange();
assert.equal(JSON.parse(result.storage.get('erp_resume_7')).y,320); // hidden-only persistence
result.context.document.hidden=true; result.events.visibilitychange();
assert.equal(JSON.parse(result.storage.get('erp_resume_7')).y,650);
result=mobile({url:'https://erp.test/rrhh/gafetes/?page=2#erp-resume',saved:{url:'/rrhh/gafetes/?page=2',y:880,mainY:100,sidebarY:50}});
result.events.load(); assert.equal(result.context.window.scrollY,880);assert.equal(result.main.scrollTop,100);
result=mobile({user:'8',active:'7',saved:{url:'/rrhh/gafetes/',y:880}});
assert(!result.storage.has('erp_resume_7'));assert(result.storage.has('erp_resume_8'));
result=mobile({standalone:false});assert(!result.storage.has('erp_resume_7'));
result=mobile({user:'',saved:{url:'/rrhh/gafetes/',y:880}});assert(!result.storage.has('erp_resume_7'));

result=mobile({loading:true,url:'https://erp.test/rrhh/gafetes/?page=2#erp-resume',saved:{url:'/rrhh/gafetes/?page=2',y:880,mainY:100}});
result.context.document.hidden=true; result.events.visibilitychange();
assert.equal(JSON.parse(result.storage.get('erp_resume_7')).y,880);
assert.equal(result.events.load,undefined);
result.events['erp:dashboard-ready']();
assert.equal(result.context.window.scrollY,880);

async function cut({hidden=false,busy=false,ok=true}={}) {
  const timers=new Map(),events={},replacements=[],toasts=[];let id=0,requests=0;
  const fragment = name=>({name});
  const context={URL,console,AbortController,
    document:{hidden,querySelector:s=>s.startsWith('form')?{dataset:{refreshAfterMs:'10000'},querySelector:()=>({})}:{replaceWith:n=>replacements.push(n.name)},addEventListener:(n,f)=>events[n]=f},
    window:{location:{href:'https://erp.test/dashboard/?sales_month=9'},setTimeout:(f,ms)=>{timers.set(++id,{f,ms});return id},clearTimeout:id=>timers.delete(id),ERPActionUI:{showToast:t=>toasts.push(t)}},
    DOMParser:class{parseFromString(){return {querySelector:s=>fragment(s.match(/"(.*?)"/)[1])}}},
    fetch:async()=>{requests++;return {ok,redirected:false,headers:{get:()=> 'application/json'},json:async()=>({busy,html:'fragments'})}}};
  vm.runInNewContext(source('dashboard_cut_refresh.js'),context);
  const poll=[...timers.entries()].find(([id,t])=>t.ms===10000);
  if(poll){timers.delete(poll[0]);await poll[1].f();}
  return {context,timers,events,replacements,toasts,requests};
}
(async()=>{
  let r=await cut();assert.equal(r.requests,1);assert.deepEqual(r.replacements,['status','sales','tickets']);assert.equal(r.timers.size,0); // terminal state schedules no further requests
  r=await cut({hidden:true});assert.equal(r.requests,0);
  r=await cut({busy:true});assert.equal(r.replacements.length,0);assert(r.timers.size>=1);
  r=await cut({ok:false});assert.equal(r.replacements.length,0);assert.equal(r.toasts[0].type,'warning');
  console.log('mobile navigation and non-reloading cut polling: PASS');
})().catch(error=>{console.error(error);process.exitCode=1});

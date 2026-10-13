(async()=>{
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const html=fs.readFileSync(`${__dirname}/templates/bonos_ventas/index.html`,'utf8');
const registros=[{id:101,dia:1,fecha:'2026-06-01',tiene_asistencia:true}];
const bono={id:1,empleado_nombre:'Prueba',sucursal:1};
const calls=[];const ref={current:null};
const c={Date,Intl,diasEnMes:(m,a)=>new Date(a,m,0).getDate(),
  h:(tag,props,...children)=>({tag,props,children}),
  gruposVentasFromBonos:()=>[['1','Sucursal']],filtrarBonosPorGrupo:x=>x,
  EmployeeSearchList:'',Badge:'',IconCheck:'',money:String,initials:()=>'',
  api:(url,options)=>{calls.push({url,options});return Promise.resolve({id:200});},
};
let state;
c.React={useState:()=>[state.shift(),()=>{}],useMemo:fn=>fn(),useEffect:()=>{},useRef:()=>ref};
vm.createContext(c);
vm.runInContext(html.slice(html.indexOf('const MESES_CORTO_BV='),html.indexOf('function normalizeText(')),c);
vm.runInContext(html.slice(html.indexOf('function CapturaTab('),html.indexOf('function PermisosTab(')),c);
c.fechaHoy=()=> '2026-06-13';
const periodo={fecha_inicio:'2026-05-28',fecha_fin:'2026-06-29'};
const fechas=c.buildDiasCal(periodo,6,2026);
assert.equal(fechas[0].key,'2026-05-28');
assert.equal(fechas.find(d=>d.key==='2026-06-01').dia,1);
function render(rows){state=['1',bono,rows,null,null,false];const tree=c.CapturaTab({bonos:[bono],mes:6,anio:2026,periodo,showStatus:()=>{}});const cells=[];
  function walk(n){if(Array.isArray(n))return n.forEach(walk);if(!n||typeof n!=='object')return;if(n.props?.className?.startsWith('day-cell '))cells.push(n);walk(n.children);}walk(tree);return cells;}
let cells=render(registros);
const may=cells.find(n=>n.props.title==='2026-05-28'),jun=cells.find(n=>n.props.title==='2026-06-01');
assert.ok(!may.props.className.includes('worked'));
assert.ok(jun.props.className.includes('worked'));
assert.equal(cells.find(n=>n.props.title==='2026-06-28').props.disabled,true);
may.props.onClick();
assert.equal(JSON.parse(calls[0].options.body).fecha,'2026-05-28');
assert.equal(JSON.parse(calls[0].options.body).dia,28);
jun.props.onClick();assert.ok(calls[1].url.endsWith('/101/'));
const pending=render([{id:102,dia:1,fecha:null,tiene_asistencia:true}]);
assert.ok(pending.every(n=>n.props.disabled),'No duplicar capturas sin fecha');
calls.length=0;
let resolveMutation;
c.api=(url,options)=>{calls.push({url,options});return new Promise(resolve=>{resolveMutation=resolve;});};
state=['1',bono,registros,null,null,false];
const tree=c.CapturaTab({bonos:[bono],mes:6,anio:2026,periodo,showStatus:()=>{}});
let cell,selector;
function find(n){if(Array.isArray(n))return n.forEach(find);if(!n||typeof n!=='object')return;if(n.props?.title==='2026-06-01')cell=n;if(n.props?.onSelect)selector=n;find(n.children);}
find(tree);
cell.props.onClick();
selector.props.onSelect({...bono,id:2});
resolveMutation({id:101});
await new Promise(resolve=>setImmediate(resolve));
assert.equal(calls.length,1,'Una respuesta de otro empleado no debe recargar ni reemplazar datos');
calls.length=0;
const effects=[],updates=[];
c.React.useEffect=fn=>effects.push(fn);
c.React.useState=()=>{const index=6-state.length;return[state.shift(),value=>{if(index===2)updates.push(value);}];};
let resolveLoad;
c.api=(url,options)=>{calls.push({url,options});return new Promise(resolve=>{resolveLoad=resolve;});};
state=['1',bono,[],null,null,false];
const loadingTree=c.CapturaTab({bonos:[bono],mes:6,anio:2026,periodo,showStatus:()=>{}});
for(const effect of effects)effect();
find(loadingTree);
selector.props.onSelect({...bono,id:2});
resolveLoad(registros);
await new Promise(resolve=>setImmediate(resolve));
assert.equal(updates.length,1,'Descartar respuesta de carga del empleado anterior');
assert.equal(updates[0].length,0);
console.log('Ventas: fechas reales, futuros, pendientes y cambios de empleado protegidos');

})().catch(error=>{console.error(error);process.exitCode=1;});

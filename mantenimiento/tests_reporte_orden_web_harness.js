'use strict';
// Ejercita el consumidor real de respuestas y los paneles con red aplazada.
const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

class Node {
  constructor(tag = 'div', doc) { this.tag = tag; this.doc = doc; this.dataset = {}; this.children = []; this.events = {}; this.hidden = false; this.classList = {contains: () => this.open}; }
  appendChild(child) { child.parentElement = this; this.children.push(child); return child; }
  remove() { this.parentElement.children = this.parentElement.children.filter(child => child !== this); this.parentElement = null; }
  addEventListener(name, fn) { (this.events[name] ||= []).push(fn); }
  dispatchEvent(event) { for (const fn of this.events[event.type] || []) fn.call(this, event); }
  focus() { this.doc.focused = this; }
  setAttribute(name, value) { this[name] = value; }
  getAttribute(name) { return this[name]; }
  reportValidity() { return true; }
  closest(selector) { let node = this; while (node) { if (selector === '[data-report-order-panel]' && node.dataset.reportOrderPanel) return node; if (selector === '#mantDrawer' && node.id === 'mantDrawer') return node; node = node.parentElement; } return null; }
  walk() { return [this, ...this.children.flatMap(child => child.walk())]; }
  querySelectorAll(selector) {
    if (selector === 'form[data-async-action]' || selector === 'form[data-report-order]') return this.walk().filter(n => n.tag === 'form');
    if (selector.startsWith('textarea,select,input')) return this.walk().filter(n => n.tag === 'textarea');
    return [];
  }
  querySelector(selector) {
    if (selector.startsWith('#')) return this.walk().find(n => n.id === selector.slice(1)) || null;
    if (selector === '[type="submit"]') return this.walk().find(n => n.type === 'submit') || null;
    if (selector === '[role="status"]') return this.walk().find(n => n.role === 'status') || null;
    return null;
  }
  set innerHTML(html) {
    this.children = [];
    const id = /id="reporteOrdenResultado-(\d+)"/.exec(html);
    if (!id) return;
    const result = this.appendChild(new Node('section', this.doc)); result.id = 'reporteOrdenResultado-' + id[1];
    if (!html.includes('data-report-order')) return;
    const form = result.appendChild(new Node('form', this.doc));
    form.dataset = {reportOrder:'', reporteId:id[1], captureSnapshot:'true', actionInlineFeedback:'true'};
    form.action = '/mantenimiento/reportes/' + id[1] + '/orden/'; form.method = 'POST';
    form.values = {clave_captura:'uuid-' + id[1], descripcion:'Trabajo'};
    form.appendChild(new Node('textarea', this.doc));
    const button = form.appendChild(new Node('button', this.doc)); button.type = 'submit'; button.textContent = 'Crear orden vinculada';
    const feedback = form.appendChild(new Node('p', this.doc)); feedback.role = 'status';
  }
  set outerHTML(html) { const parent = this.parentElement; const replacement = new Node('holder', this.doc); replacement.innerHTML = html; const child = replacement.children[0]; child.parentElement = parent; parent.children[parent.children.indexOf(this)] = child; this.parentElement = null; }
}
function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return {promise, resolve, reject}; }
function markup(id, form = true) { return `<section id="reporteOrdenResultado-${id}">${form ? '<form data-report-order></form>' : 'Orden confirmada'}</section>`; }
const tick = () => new Promise(resolve => setImmediate(resolve));

async function run(fail, closed, switched, offerNext = false) {
  const document = new Node('document'); document.doc = document;
  document.createElement = tag => new Node(tag,document);
  document.contains = node => document.walk().includes(node);
  document.getElementById = id => document.walk().find(node => node.id === id) || null;
  const drawer = document.appendChild(new Node('div', document)); drawer.id = 'mantDrawer'; drawer.open = true;
  const container = drawer.appendChild(new Node('div', document));
  const request = deferred(); const bodies = [];
  const window = {location:{href:'http://localhost/mantenimiento/',origin:'http://localhost'},history:{length:1},sessionStorage:{getItem:() => null},scrollY:430,requestAnimationFrame:fn => fn(),scrollTo:(...args) => { window.scrolled = args; }};
  const context = {document, window, URL, console, FormData: class { constructor(form) { this.values = {...form.values}; } set(name, value) { this.values[name] = value; } }, CustomEvent:class { constructor(type, data) { this.type = type; this.detail = data.detail; } }, fetch: async (url, options = {}) => {
    if (!options.method) return {ok:true,json:async () => ({html:markup(/reportes\/(\d+)/.exec(url)[1])})};
    bodies.push(options.body);
    if (offerNext && bodies.length > 1) throw new Error('respuesta perdida de la segunda intervención');
    return request.promise;
  }};
  vm.createContext(context);
  vm.runInContext(fs.readFileSync('static/js/erp_actions.js','utf8'),context);
  vm.runInContext(fs.readFileSync('static/mantenimiento/reporte_orden.js','utf8'),context);
  window.showReporteOrdenPanel(container,'5'); await tick();
  const panelA = container.children[0]; const formA = panelA.querySelectorAll('form[data-report-order]')[0];
  const buttonA = formA.querySelector('[type="submit"]');
  const submit = formA.events.submit[0]({currentTarget:formA,submitter:buttonA,preventDefault(){}});
  assert.strictEqual(buttonA.disabled,true);
  if (switched) { window.showReporteOrdenPanel(container,'6'); await tick(); }
  const panelB = container.children[1]; const formB = panelB?.querySelectorAll('form[data-report-order]')[0];
  assert.strictEqual(panelA.hidden,switched); assert.ok(document.contains(formA));
  if (closed) drawer.open = false;
  if (fail) request.reject(new Error('respuesta perdida'));
  else request.resolve({ok:true,headers:{get:() => 'application/json'},json:async () => ({ok:true,target:'#reporteOrdenResultado-5',html:markup('5',offerNext)})});
  await submit;
  if (switched) assert.strictEqual(panelB.querySelectorAll('form[data-report-order]')[0],formB,'La respuesta A no debe reemplazar B');
  if (switched || closed || fail) {
    assert.strictEqual(document.focused,undefined,'A oculto/cerrado no debe cambiar el foco');
    assert.strictEqual(window.scrolled,undefined,'A oculto/cerrado no debe desplazar la pantalla');
  }
  window.showReporteOrdenPanel(container,'5'); drawer.open = true;
  assert.strictEqual(container.children[0],panelA,'Reabrir conserva el panel vivo');
  if (fail) {
    assert.strictEqual(buttonA.disabled,false,'Un fallo tardío debe permitir reintentar A');
    assert.strictEqual(formA._captureSnapshot,bodies[0]);
    const second = formA.events.submit[0]({currentTarget:formA,submitter:buttonA,preventDefault(){}});
    await second;
    assert.strictEqual(bodies[1],bodies[0],'Reintento conserva exactamente el snapshot');
  } else if (offerNext) {
    const nextForm = panelA.querySelectorAll('form[data-report-order]')[0];
    assert.ok(nextForm.events['erp:action-start']?.length, 'El formulario reemplazado debe recuperar listeners de captura');
    assert.ok(nextForm.events['erp:action-error']?.length, 'El formulario reemplazado debe recuperar feedback inline');
    const nextButton = nextForm.querySelector('[type="submit"]');
    await nextForm.events.submit[0]({currentTarget:nextForm,submitter:nextButton,preventDefault(){}});
    assert.ok(nextForm.querySelectorAll('textarea,select,input')[0].disabled,'Respuesta incierta bloquea los datos de la nueva intervención');
    assert.match(nextForm.walk().find(node => node.role === 'alert').textContent,/Conservamos este intento/);
    assert.strictEqual(nextButton.disabled,false,'La nueva intervención permite reintentar el mismo envío');
  } else {
    assert.ok(panelA.querySelector('#reporteOrdenResultado-5'));
    assert.strictEqual(panelA.querySelectorAll('form[data-report-order]').length,0);
  }
}
(async () => { for (const fail of [false,true]) for (const closed of [false,true]) for (const switched of [false,true]) await run(fail,closed,switched); await run(false,true,true,true); console.log('PASS: éxito/fallo tardío, switch A→B, cierre/reapertura y snapshot conservado'); })().catch(error => { console.error(error); process.exitCode = 1; });

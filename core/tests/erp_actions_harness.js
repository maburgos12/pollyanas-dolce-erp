"use strict";

const fs = require("fs");
const vm = require("vm");
const assert = require("assert");

function element(tag) {
  return {
    tag, children: [], dataset: {}, disabled: false, textContent: "", className: "",
    setAttribute() {}, addEventListener() {}, remove() {}, appendChild(child) { this.children.push(child); }
  };
}

async function scenario(payload, fetchImpl, sharedStorage, hasToastRegion = true, timeoutMs = 0) {
  const events = [];
  const assignedUrls = [];
  const region = element("region");
  region.appendChild = function (child) { this.children.push(child); events.push("toast"); };
  const submitter = element("button");
  submitter.textContent = "Aprobar y aplicar";
  submitter.dataset.pendingLabel = "Procesando…";
  const otherButton = element("button");
  otherButton.textContent = "Rechazar";
  const field = { value: "dato sin perder" };
  let listener;
  let timeoutCallback;
  const form = {
    dataset: {timeoutMs: String(timeoutMs)}, method: "post", reportValidity: () => true,
    getAttribute: () => "/accion/", querySelector: () => submitter,
    addEventListener: (name, fn) => { if (name === "submit") listener = fn; }, field
  };
  const document = {
    getElementById: (id) => hasToastRegion && id === "erp-toast-region" ? region : null,
    querySelectorAll: (selector) => selector === "form[data-async-action]" ? [form] : [],
    querySelector: () => null, createElement: element, contains: () => true,
    addEventListener() {}
  };
  let fetchCount = 0;
  const storage = sharedStorage || new Map();
  const sessionStorage = {
    setItem: (key, value) => storage.set(key, value),
    getItem: (key) => storage.has(key) ? storage.get(key) : null,
    removeItem: (key) => storage.delete(key)
  };
  const context = {
    document,
    FormData: function (form) { this.value = form.field.value; this.set = function () {}; },
    CustomEvent: function (type, options) { this.type = type; this.detail = options.detail; },
    URL, AbortController,
    fetch: async (url, options) => {
      fetchCount += 1;
      return fetchImpl ? fetchImpl(options) : {
        ok: true, redirected: false, url: "https://erp.local/accion/",
        headers: { get: () => "application/json; charset=utf-8" }, json: async () => payload
      };
    },
    window: {
      location: {
        href: "https://erp.local/lista/", origin: "https://erp.local", hash: "",
        assign: (url) => { assignedUrls.push(url); events.push("navigate"); }, reload: () => events.push("reload")
      },
      setTimeout(fn, ms) { if (ms === 20000) timeoutCallback = fn; else fn(); return 1; }, clearTimeout() {}, sessionStorage, ERPActionUI: null
    },
    console
  };
  vm.runInNewContext(fs.readFileSync("static/js/erp_actions.js", "utf8"), context);
  const event = { currentTarget: form, submitter, preventDefault() {} };
  return { events, assignedUrls, form, submitter, otherButton, field, listener, event, storage, getFetchCount: () => fetchCount, fireTimeout: () => timeoutCallback() };
}

(async () => {
  const local = await scenario({ ok: true, toast: { message: "ok" }, redirect: "/destino/" });
  await local.listener(local.event);
  assert.deepStrictEqual(local.events, ["navigate"]);
  assert.strictEqual(local.submitter.disabled, true);
  assert.strictEqual(local.submitter.textContent, "Procesando…");
  const destination = await scenario(null, null, local.storage);
  assert.deepStrictEqual(destination.events, ["toast"]);
  const reload = await scenario(null, null, local.storage);
  assert.deepStrictEqual(reload.events, []);

  const sameDocument = await scenario({
    ok: true, toast: { message: "actualizado" }, redirect: "/lista/#fila-1", reload: true
  });
  await sameDocument.listener(sameDocument.event);
  assert.deepStrictEqual(sameDocument.events, ["reload"]);
  assert.strictEqual(sameDocument.storage.size, 1);

  const anotherPage = await scenario({
    ok: true, toast: { message: "actualizado" },
    redirect: "/compras/departamentales/123/#item-9", reload: true
  });
  await anotherPage.listener(anotherPage.event);
  assert.deepStrictEqual(anotherPage.events, ["navigate"]);
  assert.deepStrictEqual(anotherPage.assignedUrls, ["https://erp.local/compras/departamentales/123/#item-9"]);

  const anotherQuery = await scenario({
    ok: true, toast: { message: "actualizado" }, redirect: "/lista/?tab=2#item-9", reload: true
  });
  await anotherQuery.listener(anotherQuery.event);
  assert.deepStrictEqual(anotherQuery.events, ["navigate"]);
  assert.deepStrictEqual(anotherQuery.assignedUrls, ["https://erp.local/lista/?tab=2#item-9"]);

  const reloadHere = await scenario({ ok: true, toast: { message: "actualizado" }, reload: true });
  await reloadHere.listener(reloadHere.event);
  assert.deepStrictEqual(reloadHere.events, ["reload"]);

  const external = await scenario({ ok: true, toast: { message: "ok" }, redirect: "https://evil.example/" });
  await external.listener(external.event);
  assert.deepStrictEqual(external.events, ["toast"]);
  assert.strictEqual(external.submitter.disabled, false);
  assert.strictEqual(external.submitter.textContent, "Aprobar y aplicar");

  for (const unsafe of ["javascript:alert(1)", "https://evil.example/", "//evil.example/x", "https://erp.local:444/x", "https://user:pass@erp.local/x", "http://["]) {
    const rejected = await scenario({ ok: true, toast: { message: "ok" }, redirect: unsafe });
    await rejected.listener(rejected.event);
    assert.deepStrictEqual(rejected.events, ["toast"]);
    assert.strictEqual(rejected.submitter.disabled, false);
  }

  const login = await scenario(null, () => ({
    ok: true, redirected: true, url: "https://erp.local/login/?next=/accion/",
    headers: { get: () => "text/html" }
  }));
  await login.listener(login.event);
  assert.deepStrictEqual(login.events, ["navigate"]);
  assert.strictEqual(login.storage.size, 0);
  const loginPage = await scenario(null, null, login.storage, false);
  assert.deepStrictEqual(loginPage.events, []);
  const afterLogin = await scenario(null, null, login.storage);
  assert.deepStrictEqual(afterLogin.events, []);

  const html = await scenario(null, () => ({
    ok: true, redirected: false, url: "https://erp.local/accion/", headers: { get: () => "text/html" }
  }));
  await html.listener(html.event);
  assert.deepStrictEqual(html.events, ["toast"]);
  assert.strictEqual(html.submitter.disabled, false);

  const failed = await scenario(null, async () => { throw new Error("network"); });
  await failed.listener(failed.event);
  assert.strictEqual(failed.submitter.disabled, false);
  assert.strictEqual(failed.submitter.textContent, "Aprobar y aplicar");
  assert.strictEqual(failed.field.value, "dato sin perder");

  const timedOut = await scenario(null, ({signal}) => new Promise((resolve, reject) => {
    signal.addEventListener("abort", () => reject(Object.assign(new Error("timeout"), {name:"AbortError"})));
  }), null, true, 20000);
  const waiting = timedOut.listener(timedOut.event);
  assert.strictEqual(timedOut.submitter.disabled, true);
  timedOut.fireTimeout();
  await waiting;
  assert.strictEqual(timedOut.submitter.disabled, false);
  assert.strictEqual(timedOut.field.value, "dato sin perder");
  assert.deepStrictEqual(timedOut.events, ["toast"]);

  let release;
  const pending = await scenario(null, () => new Promise((resolve) => { release = resolve; }));
  const first = pending.listener(pending.event);
  await Promise.resolve();
  await pending.listener(pending.event);
  assert.strictEqual(pending.getFetchCount(), 1);
  assert.strictEqual(pending.submitter.disabled, true);
  assert.strictEqual(pending.otherButton.disabled, false);
  release({ ok: true, redirected: false, headers: { get: () => "application/json" }, json: async () => ({ ok: true, toast: { message: "ok" } }) });
  await first;
  const bodies = [];
  let attempt = 0;
  const capture = await scenario(null, async (options) => {
    bodies.push(options.body);
    attempt += 1;
    if (attempt === 1) throw new Error("response lost");
    return { status: 200, ok: true, headers: { get: () => "application/json" }, json: async () => ({ok: true}) };
  });
  capture.form.dataset.captureSnapshot = "true";
  const captureEvents = [];
  capture.form.dispatchEvent = (event) => captureEvents.push(event);
  await capture.listener(capture.event);
  capture.field.value = "changed after uncertain request";
  await capture.listener(capture.event);
  assert.strictEqual(bodies[0], bodies[1]);
  assert.strictEqual(bodies[1].value, "dato sin perder");
  assert.deepStrictEqual(captureEvents.map(e => e.type), ["erp:action-start", "erp:action-error", "erp:action-start", "erp:action-success"]);
  const invalid = await scenario(null, async () => ({ status: 400, ok: false, headers: { get: () => "application/json" }, json: async () => ({ok: false}) }));
  invalid.form.dataset.captureSnapshot = "true";
  invalid.form.dispatchEvent = () => {};
  await invalid.listener(invalid.event);
  assert.strictEqual(invalid.form._captureSnapshot, null);
  const conflict = await scenario(null, async () => ({ status: 409, ok: false, headers: { get: () => "application/json" }, json: async () => ({ok: false}) }));
  conflict.form.dataset.captureSnapshot = "true";
  conflict.form.dispatchEvent = () => {};
  await conflict.listener(conflict.event);
  assert.ok(conflict.form._captureSnapshot);
  const captureLogin = await scenario(null, async () => ({ status: 200, ok: true, redirected: true, url: "https://erp.local/login/", headers: { get: () => "text/html" } }));
  captureLogin.form.dataset.captureSnapshot = "true";
  captureLogin.form.dispatchEvent = () => {};
  await captureLogin.listener(captureLogin.event);
  assert.deepStrictEqual(captureLogin.events, ["toast"]);
  assert.ok(captureLogin.form._captureSnapshot);
  for (const statusCode of [400, 403, 404]) {
    const unknown = await scenario(null, async () => ({ status: statusCode, ok: false, redirected: false, headers: { get: () => "text/html" } }));
    unknown.form.dataset.captureSnapshot = "true";
    const unknownEvents = [];
    unknown.form.dispatchEvent = (event) => unknownEvents.push(event);
    await unknown.listener(unknown.event);
    assert.ok(unknown.form._captureSnapshot, `HTML ${statusCode} debe conservar el envío original`);
    assert.strictEqual(unknownEvents[1].detail.statusCode, 0);
  }
  const inlineError = await scenario(null, async () => { throw new Error("response lost"); });
  inlineError.form.dataset.captureSnapshot = "true";
  inlineError.form.dataset.actionInlineFeedback = "true";
  const inlineErrorEvents = [];
  inlineError.form.dispatchEvent = (event) => inlineErrorEvents.push(event);
  await inlineError.listener(inlineError.event);
  assert.deepStrictEqual(inlineError.events, [], "El feedback inline no debe duplicarse con un toast que tape Guardar");
  assert.strictEqual(inlineErrorEvents[1].type, "erp:action-error");
  assert.ok(inlineErrorEvents[1].detail.message);
  assert.ok(inlineError.form._captureSnapshot);
  const inlineSuccess = await scenario({ok: true, toast: {message: "Guardado"}});
  inlineSuccess.form.dataset.captureSnapshot = "true";
  inlineSuccess.form.dataset.actionInlineFeedback = "true";
  const inlineSuccessEvents = [];
  inlineSuccess.form.dispatchEvent = (event) => inlineSuccessEvents.push(event);
  await inlineSuccess.listener(inlineSuccess.event);
  assert.deepStrictEqual(inlineSuccess.events, []);
  assert.strictEqual(inlineSuccessEvents[1].type, "erp:action-success");
  console.log("erp_actions harness: ok");
})().catch((error) => { console.error(error); process.exit(1); });

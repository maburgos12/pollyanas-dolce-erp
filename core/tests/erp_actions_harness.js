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

async function scenario(payload, fetchImpl, sharedStorage, hasToastRegion = true) {
  const events = [];
  const region = element("region");
  region.appendChild = function (child) { this.children.push(child); events.push("toast"); };
  const submitter = element("button");
  submitter.textContent = "Aprobar y aplicar";
  submitter.dataset.pendingLabel = "Procesando…";
  const otherButton = element("button");
  otherButton.textContent = "Rechazar";
  const field = { value: "dato sin perder" };
  let listener;
  const form = {
    dataset: {}, method: "post", reportValidity: () => true,
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
    FormData: function () { this.set = function () {}; },
    URL,
    fetch: async () => {
      fetchCount += 1;
      return fetchImpl ? fetchImpl() : {
        ok: true, redirected: false, url: "https://erp.local/accion/",
        headers: { get: () => "application/json; charset=utf-8" }, json: async () => payload
      };
    },
    window: {
      location: {
        href: "https://erp.local/lista/", origin: "https://erp.local", hash: "",
        assign: () => events.push("navigate"), reload: () => events.push("reload")
      },
      setTimeout(fn) { fn(); }, sessionStorage, ERPActionUI: null
    },
    console
  };
  vm.runInNewContext(fs.readFileSync("static/js/erp_actions.js", "utf8"), context);
  const event = { currentTarget: form, submitter, preventDefault() {} };
  return { events, form, submitter, otherButton, field, listener, event, storage, getFetchCount: () => fetchCount };
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
  console.log("erp_actions harness: ok");
})().catch((error) => { console.error(error); process.exit(1); });

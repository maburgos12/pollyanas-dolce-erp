(function () {
  "use strict";
  var form = document.querySelector("form[data-cut-refresh-poll]");
  if (!form || !form.querySelector('[aria-busy="true"]')) return;
  var interval = Math.max(3000, Number(form.dataset.refreshAfterMs) || 10000);
  var timer, pending = false, done = false, warned = false;
  var url = new URL(window.location.href);
  url.searchParams.delete("source");
  url.searchParams.set("cut_refresh", "1");
  function schedule() { if (!done && !document.hidden) timer = window.setTimeout(poll, interval); }
  async function poll() {
    if (done || pending || document.hidden) return;
    pending = true;
    var controller = new AbortController();
    var timeout = window.setTimeout(function () { controller.abort(); }, 15000);
    try {
      var response = await fetch(url.href, {credentials: "same-origin", cache: "no-store", signal: controller.signal});
      if (!response.ok || response.redirected || !(response.headers.get("content-type") || "").includes("application/json")) throw new Error("cut-status");
      var result = await response.json();
      if (!result.busy) {
        var updated = new DOMParser().parseFromString(result.html, "text/html");
        var fragments = ["status", "sales", "tickets"];
        if (!fragments.every(function (name) { return updated.querySelector('[data-cut-fragment="' + name + '"]'); })) throw new Error("cut-fragments");
        fragments.forEach(function (name) {
          var current = document.querySelector('[data-cut-fragment="' + name + '"]');
          if (current) current.replaceWith(updated.querySelector('[data-cut-fragment="' + name + '"]'));
        });
        done = true;
        if (window.ERPActionUI) window.ERPActionUI.showToast(result.toast || {message: "Estado del corte actualizado.", type: "info"});
      }
    } catch (_) {
      if (!warned && window.ERPActionUI) window.ERPActionUI.showToast({message: "No se pudo comprobar el corte. Se volverá a intentar.", type: "warning"});
      warned = true;
    } finally {
      window.clearTimeout(timeout);
      pending = false;
      schedule();
    }
  }
  document.addEventListener("visibilitychange", function () {
    window.clearTimeout(timer);
    if (!document.hidden) poll();
  });
  schedule();
})();

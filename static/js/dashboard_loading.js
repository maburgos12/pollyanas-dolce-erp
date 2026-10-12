(function () {
  "use strict";
  var root = document.querySelector("[data-dashboard-loading]");
  if (!root) return;
  var status = root.querySelector("[data-loading-status]");
  var retry = root.querySelector("[data-loading-retry]");
  var fallback = root.querySelector("[data-loading-fallback]");
  var track = root.querySelector("[data-loading-track]");
  var pending = false;
  var url = new URL(root.dataset.url, location.href);
  history.replaceState(null, "", url.pathname + url.search + location.hash);

  function asset(node) {
    return new Promise(function (resolve, reject) {
      var copy = document.createElement(node.tagName);
      Array.from(node.attributes).forEach(function (attr) { copy.setAttribute(attr.name, attr.value); });
      copy.removeAttribute("defer");
      copy.textContent = node.textContent;
      var external = node.tagName === "LINK" || (node.tagName === "SCRIPT" && node.src);
      var timer;
      if (external) {
        timer = window.setTimeout(function () { copy.remove(); reject(new Error("asset-timeout")); }, 10000);
        copy.onload = function () { window.clearTimeout(timer); resolve(); };
        copy.onerror = function () { window.clearTimeout(timer); copy.remove(); reject(new Error("asset-load")); };
      }
      document.head.appendChild(copy);
      if (!external) resolve();
    });
  }

  async function load() {
    if (pending) return;
    pending = true;
    retry.hidden = true;
    fallback.hidden = true;
    track.hidden = false;
    root.setAttribute("aria-busy", "true");
    status.textContent = "Cargando datos del Dashboard…";
    var controller = new AbortController();
    var timeout = window.setTimeout(function () { controller.abort(); }, 90000);
    var waiting = window.setTimeout(function () { status.textContent = "Seguimos preparando la información. Puedes usar el menú mientras carga."; }, 4000);
    try {
      var response = await fetch(url.href, {credentials: "same-origin", cache: "no-store", headers: {"X-ERP-Dashboard": "fragment"}, signal: controller.signal});
      if (response.redirected) {
        var destination = new URL(response.url);
        if (destination.origin === location.origin && destination.pathname === "/login/") {
          fallback.href = destination.href;
          fallback.textContent = "Iniciar sesión";
          throw new Error("session-expired");
        }
        throw new Error("dashboard-access");
      }
      if (!response.ok || !(response.headers.get("content-type") || "").includes("text/html")) throw new Error("dashboard-response");
      var html = await response.text();
      var parsed = new DOMParser().parseFromString(html, "text/html");
      var content = parsed.querySelector("template[data-dashboard-content]");
      var assets = parsed.querySelector("template[data-dashboard-assets]");
      if (!content || !assets) throw new Error("dashboard-fragments");
      window.clearTimeout(waiting);
      status.textContent = "Organizando los datos del Dashboard…";
      await Promise.all(Array.from(assets.content.querySelectorAll('link[rel="stylesheet"]')).map(asset));
      root.replaceChildren(content.content.cloneNode(true));
      root.removeAttribute("data-dashboard-loading");
      root.setAttribute("aria-busy", "false");
      window.dispatchEvent(new Event("erp:dashboard-ready"));
      var scripts = Array.from(assets.content.querySelectorAll("script"));
      var chartWarning = false;
      for (var script of scripts) {
        try { await asset(script); }
        catch (_) { chartWarning = true; }
      }
      if (chartWarning && window.ERPActionUI) window.ERPActionUI.showToast({type: "warning", message: "Algunos gráficos no pudieron cargar. Puedes consultar los datos en las tablas."});
    } catch (error) {
      status.textContent = error.message === "session-expired" ? "Tu sesión terminó. Inicia sesión para consultar los datos." : "No pudimos cargar los datos. Puedes reintentar sin salir de esta pantalla.";
      root.setAttribute("aria-busy", "false");
      track.hidden = true;
      retry.hidden = error.message === "session-expired";
      fallback.hidden = false;
    } finally {
      window.clearTimeout(timeout);
      window.clearTimeout(waiting);
      pending = false;
    }
  }
  retry.addEventListener("click", load);
  load();
})();

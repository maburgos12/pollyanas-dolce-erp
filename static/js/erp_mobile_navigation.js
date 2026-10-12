(function () {
  "use strict";
  var script = document.currentScript;
  var user = script.dataset.user;
  var activeKey = "erp_resume_active";
  function clear(userId) {
    if (!/^\d+$/.test(userId || "")) return;
    try { localStorage.removeItem("erp_resume_" + userId); } catch (_) {}
    document.cookie = "erp_resume_" + userId + "=; Max-Age=0; Path=/; SameSite=Lax";
  }
  try {
    var previous = localStorage.getItem(activeKey);
    if (previous !== user) clear(previous);
    if (user) localStorage.setItem(activeKey, user);
    else localStorage.removeItem(activeKey);
  } catch (_) {}
  if (!user || !(navigator.standalone || window.matchMedia("(display-mode: standalone)").matches)) return;
  if (script.dataset.method !== "GET") return;
  var current = new URL(window.location.href);
  // shortcut: only menu destinations resume; extend after validating detail/form recovery per module.
  var allowed = Array.from(document.querySelectorAll(".sidebar-nav a[href]")).some(function (link) {
    return new URL(link.href).pathname === current.pathname;
  });
  if (!allowed) return;
  var filters = ["sales_year", "sales_month", "sales_branch", "months", "page", "sucursal", "branch_id", "tab", "periodo", "fecha", "status", "estatus", "year", "month"];
  var query = new URLSearchParams();
  current.searchParams.forEach(function (value, key) {
    if (filters.indexOf(key) !== -1 && /^[A-Za-z0-9_-]{1,64}$/.test(value)) query.append(key, value);
  });
  var url = current.pathname + (query.toString() ? "?" + query.toString() : "");
  var key = "erp_resume_" + user;
  var main = document.querySelector(".main-content");
  var sidebar = document.querySelector(".sidebar-nav");
  var ready = !document.querySelector("[data-dashboard-loading]");
  var saved;
  try { saved = JSON.parse(localStorage.getItem(key)); } catch (_) {}
  var resume = current.hash === "#erp-resume";
  var navigation = performance.getEntriesByType("navigation")[0];
  if (resume) history.replaceState(null, "", current.pathname + current.search);
  if (saved && saved.url === url && (resume || (navigation && navigation.type === "reload"))) {
    window.addEventListener(ready ? "load" : "erp:dashboard-ready", function () {
      window.scrollTo(0, Math.max(0, Number(saved.y) || 0));
      if (main) main.scrollTop = Math.max(0, Number(saved.mainY) || 0);
      if (sidebar) sidebar.scrollTop = Math.max(0, Number(saved.sidebarY) || 0);
    }, {once: true});
  }
  var closing = false;
  window.addEventListener("erp:dashboard-ready", function () { ready = true; }, {once: true});
  function save() {
    if (closing || !ready) return;
    try {
      localStorage.setItem(key, JSON.stringify({url: url, y: window.scrollY, mainY: main ? main.scrollTop : 0, sidebarY: sidebar ? sidebar.scrollTop : 0}));
      document.cookie = key + "=" + encodeURIComponent(url) + "; Max-Age=86400; Path=/; SameSite=Lax" + (location.protocol === "https:" ? "; Secure" : "");
    } catch (_) {}
  }
  var timer;
  document.addEventListener("scroll", function () {
    window.clearTimeout(timer);
    timer = window.setTimeout(save, 150);
  }, true);
  document.addEventListener("visibilitychange", function () { if (document.hidden) save(); });
  window.addEventListener("pagehide", save);
  document.addEventListener("click", function (event) {
    var link = event.target.closest("a[href]");
    if (link && new URL(link.href).pathname === "/logout/") {
      closing = true; clear(user);
      try { localStorage.removeItem(activeKey); } catch (_) {}
    }
  });
  if (!(saved && saved.url === url && (resume || (navigation && navigation.type === "reload")))) save();
})();

const CACHE_NAME = "pollyanas-logistica-pwa-v95-evidencias-network-only";
const SHELL_ASSETS = [
  "/logistica/app/",
  "/static/logistica/pwa/offline_queue_compat.js?v=route-control-v63-carga-tramos-consolidada",
  "/static/logistica/pwa/manifest.json?v=20260707-workflow-icon-v5",
  "/static/operacion/app-icon-192.png?v=20260707-workflow-icon-v5",
  "/static/operacion/app-icon-512.png?v=20260707-workflow-icon-v5"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  if (url.pathname.startsWith("/media/")) {
    // Reauthorize historical files online; retain offline copies of public hot media only.
    event.respondWith(
      fetch(event.request).then(async (response) => {
        try {
          const cache = await caches.open(CACHE_NAME);
          const privateMedia = /private|no-store/i.test(response.headers.get("Cache-Control") || "");
          if (!response.ok || privateMedia) {
            await cache.delete(event.request);
          } else if (event.request.method === "GET") {
            await cache.put(event.request, response.clone());
          }
        } catch (_) { /* Cache failures must never replace the server's authorization result. */ }
        return response;
      }, () => caches.match(event.request).then((cached) => {
        if (cached && !/private|no-store/i.test(cached.headers.get("Cache-Control") || "")) return cached;
        return Response.error();
      }))
    );
    return;
  }

  if (url.pathname.startsWith("/api/")) {
    event.respondWith(fetch(event.request));
    return;
  }

  if (event.request.mode === "navigate" || url.pathname === "/logistica/app/") {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          if (event.request.method === "GET" && response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
          }
          return response;
        })
        .catch(() => caches.match(event.request).then((cached) => cached || caches.match("/logistica/app/")))
    );
    return;
  }

  event.respondWith(
    caches.match(event.request).then((cached) => {
      if (cached) return cached;
      return fetch(event.request).then((response) => {
        if (event.request.method === "GET" && response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
        }
        return response;
      });
    })
  );
});

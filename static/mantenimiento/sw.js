const CACHE_PREFIX = "pollyanas-mantenimiento-pwa-";
const CACHE_VERSION = "20260721-cierre-costo-v3";
const CACHE_NAME = `${CACHE_PREFIX}v20-${CACHE_VERSION}`;
const SHELL_ASSETS = [
  "/static/mantenimiento/manifest.json?v=20260707-workflow-icon-v5",
  "/static/operacion/app-icon-192.png?v=20260707-workflow-icon-v5",
  "/static/operacion/app-icon-512.png?v=20260707-workflow-icon-v5"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(SHELL_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys
          .filter((key) => key.startsWith(CACHE_PREFIX) && key !== CACHE_NAME)
          .map((key) => caches.delete(key))
      ))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (
    event.request.mode === "navigate" ||
    url.pathname.startsWith("/mantenimiento/") ||
    url.pathname.startsWith("/api/") ||
    url.pathname.startsWith("/media/")
  ) {
    event.respondWith(fetch(event.request));
    return;
  }

  const isCacheableRequest =
    event.request.method === "GET" &&
    url.origin === self.location.origin &&
    url.pathname.startsWith("/static/") &&
    !event.request.headers.has("Authorization");

  if (!isCacheableRequest) {
    event.respondWith(fetch(event.request));
    return;
  }

  event.respondWith(
    caches.match(event.request).then((cached) => {
      if (cached) return cached;
      return fetch(event.request).then((response) => {
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
        }
        return response;
      });
    })
  );
});

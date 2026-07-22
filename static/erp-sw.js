const CACHE_NAME = "pollyanas-erp-shell-v22-rrhh-alta-usuario";
const INSTALL_ASSETS = [
  "/static/manifest.webmanifest",
  "/static/favicon-192x192.png?v=20260525-logo-v1",
  "/static/pwa-icon-512.png?v=20260622-erp-pwa-v1"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(INSTALL_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
      ))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  event.respondWith(fetch(event.request));
});

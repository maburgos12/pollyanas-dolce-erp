const CACHE_NAME = "pollyanas-app-operativa-pwa-v40-pasaporte-activos-qr";
const SHELL_ASSETS = [
  "/static/operacion/manifest.webmanifest?v=20260708-mobile-polish-v4",
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
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => key.startsWith("pollyanas-app-operativa-pwa-") && key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.pathname.startsWith("/app/conteos/") || url.pathname.startsWith("/inventario/conteos-sucursales/")) return;
  if (url.pathname.startsWith("/api/") || url.pathname.startsWith("/app/api/")) return;
  // El pasaporte y la búsqueda manual son respuestas autenticadas y con alcance
  // por usuario: servirlas desde caché mostraría datos vencidos como vigentes, o
  // los de otra sesión en un teléfono compartido.
  if (url.pathname.startsWith("/app/activos/")) return;
  if (event.request.mode === "navigate") {
    event.respondWith(fetch(event.request));
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

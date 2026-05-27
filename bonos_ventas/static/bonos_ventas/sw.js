const CACHE_NAME = "pollyanas-bonos-ventas-pwa-v3";
const SHELL_ASSETS = [
  "/bonos-ventas/manifest.json",
  "/static/bonos_ventas/icons/icon-192.png",
  "https://unpkg.com/react@18/umd/react.production.min.js",
  "https://unpkg.com/react-dom@18/umd/react-dom.production.min.js",
  "https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&family=Playfair+Display:ital,wght@0,700;1,400&display=swap"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => Promise.all(
        SHELL_ASSETS.map((asset) => {
          const request = asset.startsWith("http") ? new Request(asset, {mode: "no-cors"}) : asset;
          return cache.add(request).catch(() => null);
        })
      ))
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
  const url = new URL(event.request.url);
  if (url.pathname.startsWith("/api/")) {
    event.respondWith(fetch(event.request));
    return;
  }
  if (event.request.method !== "GET") return;

  const acceptsHtml = event.request.headers.get("accept")?.includes("text/html");
  const isBonosHtml = url.pathname === "/bonos-ventas/app/" || url.pathname.startsWith("/bonos-ventas/dashboard/");

  if (isBonosHtml || acceptsHtml || url.pathname === "/bonos-ventas/sw.js") {
    event.respondWith(
      fetch(event.request, {cache: "no-store"}).catch(() => caches.match(event.request))
    );
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

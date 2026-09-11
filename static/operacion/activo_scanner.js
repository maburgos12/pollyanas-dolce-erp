/* Lector de etiquetas QR de activos.
 *
 * Sólo acepta URLs de este mismo origen que apunten a /app/activos/q/<uuid>/.
 * Un QR de cualquier otra procedencia —un empaque, un cartel, una etiqueta de
 * otro sistema— se ignora en vez de navegar a donde diga.
 *
 * No se guarda video, foto ni ubicación: la cámara se enciende al tocar el
 * botón y sus tracks se detienen al salir o al leer algo válido.
 */
(() => {
  const raiz = document.querySelector(".scanner");
  if (!raiz || typeof QrScanner === "undefined") return;

  const video = raiz.querySelector("[data-video]");
  const visor = raiz.querySelector("[data-visor]");
  const iniciar = raiz.querySelector("[data-iniciar]");
  const detener = raiz.querySelector("[data-detener]");
  const estado = raiz.querySelector("[data-estado]");
  const entradaFoto = raiz.querySelector("[data-foto]");
  const formCodigo = raiz.querySelector("[data-codigo-form]");
  const campoCodigo = raiz.querySelector("[data-codigo]");
  const botonBuscar = raiz.querySelector("[data-buscar]");
  const buscarUrl = raiz.dataset.buscarUrl;

  QrScanner.WORKER_PATH = raiz.dataset.workerUrl;

  const RUTA_PASAPORTE = /^\/app\/activos\/q\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\/$/i;
  let lector = null;
  let navegando = false;

  function decir(mensaje, tono = "info") {
    estado.textContent = mensaje;
    estado.dataset.tono = tono;
  }

  function rutaDePasaporte(texto) {
    let url;
    try {
      url = new URL(texto, window.location.origin);
    } catch {
      return null;
    }
    if (url.origin !== window.location.origin) return null;
    if (!RUTA_PASAPORTE.test(url.pathname)) return null;
    return url.pathname;
  }

  function apagar() {
    if (!lector) return;
    lector.stop();
    lector.destroy();
    lector = null;
    visor.hidden = true;
    detener.hidden = true;
    iniciar.hidden = false;
  }

  function abrir(ruta) {
    if (navegando) return;
    navegando = true;
    apagar();
    decir("Activo identificado; abriendo su ficha…", "ok");
    window.location.href = ruta;
  }

  function leido(resultado) {
    const texto = typeof resultado === "string" ? resultado : resultado?.data;
    const ruta = rutaDePasaporte(texto || "");
    if (!ruta) {
      decir("Ese QR no es una etiqueta de activo de Pollyana's Dolce.", "warn");
      return;
    }
    abrir(ruta);
  }

  function mensajeDeError(error) {
    const nombre = error?.name || "";
    const texto = String(error?.message || error || "");
    if (nombre === "NotAllowedError" || /permission|denied/i.test(texto)) {
      return "No diste permiso de cámara. Puedes subir una foto del QR o escribir el código.";
    }
    if (nombre === "NotFoundError" || /no camera|not found/i.test(texto)) {
      return "Este dispositivo no tiene cámara disponible. Usa la foto o el código.";
    }
    if (nombre === "NotReadableError" || /in use|could not start/i.test(texto)) {
      return "La cámara está ocupada por otra app. Ciérrala e inténtalo de nuevo.";
    }
    if (nombre === "SecurityError") {
      return "El navegador bloqueó la cámara en esta página. Usa la foto o el código.";
    }
    return "No fue posible encender la cámara. Usa la foto del QR o escribe el código.";
  }

  iniciar?.addEventListener("click", async () => {
    iniciar.disabled = true;
    decir("Pidiendo permiso de cámara…");
    try {
      lector = new QrScanner(video, leido, {
        preferredCamera: "environment",
        highlightScanRegion: true,
        highlightCodeOutline: true,
        returnDetailedScanResult: true,
      });
      await lector.start();
      visor.hidden = false;
      detener.hidden = false;
      iniciar.hidden = true;
      decir("Apunta a la etiqueta del equipo.");
    } catch (error) {
      apagar();
      decir(mensajeDeError(error), "warn");
    } finally {
      iniciar.disabled = false;
    }
  });

  detener?.addEventListener("click", () => {
    apagar();
    decir("Cámara apagada.");
  });

  entradaFoto?.addEventListener("change", async () => {
    const archivo = entradaFoto.files?.[0];
    if (!archivo) return;
    decir("Leyendo la foto…");
    try {
      const resultado = await QrScanner.scanImage(archivo, { returnDetailedScanResult: true });
      leido(resultado);
    } catch {
      decir("No se encontró un QR legible en esa foto. Prueba con más luz o escribe el código.", "warn");
    } finally {
      entradaFoto.value = "";
    }
  });

  formCodigo?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const codigo = (campoCodigo.value || "").trim();
    if (!codigo) return;
    botonBuscar.disabled = true;
    const original = botonBuscar.textContent;
    botonBuscar.textContent = "Buscando…";
    decir("Buscando el código…");
    try {
      const url = new URL(buscarUrl, window.location.origin);
      url.searchParams.set("codigo", codigo);
      const respuesta = await fetch(url, {
        headers: { "X-Requested-With": "XMLHttpRequest" },
        credentials: "same-origin",
        cache: "no-store",
      });
      const payload = await respuesta.json().catch(() => ({}));
      if (!respuesta.ok) throw new Error(payload.error || "No se pudo abrir ese activo.");
      abrir(payload.url);
    } catch (error) {
      const sinRed = error instanceof TypeError || !navigator.onLine;
      decir(sinRed ? "No hay conexión; no se pudo consultar el activo." : error.message, "warn");
    } finally {
      botonBuscar.disabled = false;
      botonBuscar.textContent = original;
    }
  });

  window.addEventListener("pagehide", apagar);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") apagar();
  });
})();

/* Hoja de impresión a partir del detalle autorizado y guardado del reporte. */
(() => {
  "use strict";
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;"}[c]));
  const fecha = value => value ? new Date(value).toLocaleString("es-MX", {timeZone:"America/Mazatlan"}) : "Sin registrar";
  const dinero = value => value == null || value === "" ? "Sin registrar" : Number(value).toLocaleString("es-MX", {style:"currency", currency:"MXN"});
  const urlArchivo = value => {
    if (!value) return "";
    try {
      const url = new URL(value, location.origin);
      return ["https:", "http:"].includes(url.protocol) ? esc(url.href) : "";
    } catch (_) { return ""; }
  };
  const dato = (label, value) => `<div><dt>${esc(label)}</dt><dd>${esc(value || "Sin registrar")}</dd></div>`;

  window.imprimirReporteFalla = (rep, responsable) => {
    const hoja = window.open("", "_blank");
    if (!hoja) {
      window.ERPActionUI?.showToast({type:"warning", message:"Permite las ventanas emergentes para abrir la impresión del reporte."});
      return;
    }
    hoja.opener = null;
    const foto = urlArchivo(rep.foto_evidencia);
    const bitacora = (rep.bitacora || []).map(row => `<article class="movimiento">
      <h3>${esc(row.usuario_nombre)} · ${esc(fecha(row.timestamp))}</h3>
      ${row.estatus_nuevo_display ? `<p><strong>${esc(row.estatus_nuevo_display)}</strong></p>` : ""}
      <p class="texto">${esc(row.comentario || "Sin comentario")}</p>
      ${(row.evidencias || []).map(e => { const url = urlArchivo(e.url); return url ? `<p class="adjunto">Adjunto: <a href="${url}" target="_blank" rel="noopener noreferrer">${esc(e.nombre || "Evidencia de seguimiento")}</a></p>` : ""; }).join("")}
    </article>`).join("");
    hoja.document.open();
    hoja.addEventListener("load", () => {
      const rota = [...hoja.document.images].some(img => !img.naturalWidth);
      hoja.document.getElementById("print-status").textContent = rota
        ? "No se pudo cargar la fotografía. Cierra esta hoja y vuelve a intentarlo antes de imprimir."
        : "Se imprimen los datos guardados. Puedes elegir Guardar como PDF en el diálogo de impresión.";
      hoja.document.getElementById("print-now").disabled = false;
      if (!rota) { hoja.focus(); hoja.print(); }
    }, {once:true});
    hoja.document.write(`<!doctype html><html lang="es"><head><meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Reporte de falla ${esc(rep.id)} · ${esc(rep.sucursal_nombre)}</title>
      <link rel="stylesheet" href="${location.origin}/static/fallas/imprimir_reporte.css?v=20260908-print-v1">
      </head><body>
      <div class="herramientas"><button type="button" id="print-now" disabled>Imprimir / Guardar PDF</button><p id="print-status" role="status">Preparando fotografía y formato…</p></div>
      <main><header><p class="marca">Pollyana’s Dolce</p><h1>Reporte de falla #${esc(rep.id)}</h1><p>${esc(rep.sucursal_nombre)}</p></header>
      <dl class="datos">${dato("Fecha del reporte", fecha(rep.fecha_reporte))}${dato("Reportado por", rep.reportado_por_nombre)}${dato("Área", rep.area_display || rep.area)}${dato("Categoría", rep.categoria_nombre)}${dato("Prioridad", rep.prioridad_display || rep.prioridad)}${dato("Estatus", rep.estatus_display || rep.estatus)}</dl>
      <section><h2>${esc(rep.titulo)}</h2><p class="texto">${esc(rep.descripcion)}</p></section>
      ${foto ? `<figure><img src="${foto}" alt="Fotografía de evidencia del reporte ${esc(rep.id)}"><figcaption>Evidencia del reporte</figcaption></figure>` : '<p>Fotografía: sin evidencia adjunta.</p>'}
      <section><h2>Seguimiento</h2><dl class="datos">
      ${dato("Activo", rep.activo_nombre)}${dato("Responsable asignado", responsable || (rep.asignado_a ? `Usuario #${rep.asignado_a}` : "Sin asignar"))}
      ${dato("Proveedor de servicio", rep.proveedor_servicio)}${dato("Fecha de asignación", fecha(rep.fecha_asignacion))}
      ${dato("Costo estimado (MXN)", dinero(rep.costo_estimado))}${dato("Costo real (MXN)", dinero(rep.costo_real))}
      ${dato("Fecha de resolución", fecha(rep.fecha_resolucion))}${dato("Fecha de cierre", fecha(rep.fecha_cierre))}
      </dl></section>
      <section><h2>Bitácora</h2>${bitacora || '<p>Sin historial aún.</p>'}</section>
      <footer>Impreso el ${esc(fecha(new Date()))} · Horario de Mazatlán · Folio #${esc(rep.id)}</footer>
      </main></body></html>`);
    hoja.document.getElementById("print-now").addEventListener("click", () => hoja.print());
    hoja.document.close();
  };
})();

(function () {
  const form = document.getElementById("actualizar-carga-point");
  if (!form) return;
  // El input name="action" oculta la propiedad form.action del DOM.
  const routeUrl = new URL(form.getAttribute("action"), window.location.href);
  let active = 0;
  let dirty = false;
  document.addEventListener("input", (event) => {
    if (event.target.closest("#carga-operacion-point form")) dirty = true;
  });
  const statusUrl = (id) => {
    const url = new URL(routeUrl);
    url.hash = "";
    url.search = "";
    url.searchParams.set("carga_point_estado", id);
    return url;
  };
  async function watch(id) {
    if (!id || active === id) return;
    active = id;
    const end = Date.now() + 330000;
    const button = form.querySelector("button[type=submit]");
    const widget = document.getElementById("estado-carga-point");
    const label = widget.querySelector("[data-carga-point-mensaje]");
    const reload = widget.querySelector("[data-carga-point-recargar]");
    button.disabled = true;
    reload.href = routeUrl.href;
    try {
      while (active === id && Date.now() < end) {
        let response, data;
        try {
          response = await fetch(statusUrl(id), {headers: {Accept: "application/json"}, cache: "no-store"});
          data = await response.json();
        } catch (_) {
          label.textContent = "Se perdió la conexión. La solicitud sigue registrada; consultaremos de nuevo.";
          await new Promise(resolve => setTimeout(resolve, 5000));
          continue;
        }
        if (!response.ok) throw new Error("No se pudo consultar el resultado. Revisa tu sesión y vuelve a intentarlo.");
        if (response.status === 202) {
          label.textContent = "Actualización en curso. Esperará su turno si Point está ocupado.";
          await new Promise(resolve => setTimeout(resolve, 3000));
          continue;
        }
        const messages = {
          ACTUALIZADA: "Carga actualizada desde Point.",
          PENDIENTE_ENVIO: "Este tramo aún no tiene carga enviada. Cuando CEDIS registre el despacho en Point, vuelve a actualizar desde aquí.",
          SIN_CARGA: "Point no mostró carga para este tramo; se generará una alerta para revisión."
        };
        if (!messages[data.estado]) throw new Error(data.detail || "No se pudo actualizar la carga.");
        label.textContent = data.mensaje || messages[data.estado];
        window.ERPActionUI?.showToast({type: data.estado === "ACTUALIZADA" ? "success" : "warning", message: label.textContent});
        const view = await fetch(routeUrl, {cache: "no-store"});
        if (!view.ok) throw new Error("La consulta terminó; vuelve a abrir la ruta para ver los datos.");
        const parsed = new DOMParser().parseFromString(await view.text(), "text/html");
        const fresh = parsed.getElementById("carga-operacion-point");
        if (!fresh) throw new Error("La consulta terminó; revisa tu sesión antes de recargar.");
        reload.hidden = false;
        if (dirty) {
          label.textContent += " Conservamos los campos que estás editando. Abre la carga actualizada cuando termines.";
          reload.hidden = false;
        } else {
          const filter = document.getElementById("route-load-detail-search")?.value || "";
          document.getElementById("carga-operacion-point").replaceWith(fresh);
          const search = document.getElementById("route-load-detail-search");
          if (search) search.value = filter;
          window.bindRouteLoading?.();
          window.bindCargaPointFilter?.();
          window.ERPActionUI?.bind(document);
        }
        return;
      }
      label.textContent = "La solicitud sigue registrada. Vuelve a consultar su resultado.";
      reload.hidden = false;
    } catch (error) {
      label.textContent = error.message;
      window.ERPActionUI?.showToast({type: "error", message: error.message, persistent: true});
      reload.hidden = false;
    } finally {
      if (active === id) active = 0;
      button.disabled = false;
    }
  }
  form.addEventListener("erp:action-success", event => {
    watch(event.detail.solicitud_id);
    // El contrato global termina su POST; la espera de Point mantiene sólo este botón ocupado.
    setTimeout(() => { if (active) form.querySelector("button[type=submit]").disabled = true; }, 0);
  });
  watch(Number(document.getElementById("estado-carga-point")?.dataset.solicitudId));
})();

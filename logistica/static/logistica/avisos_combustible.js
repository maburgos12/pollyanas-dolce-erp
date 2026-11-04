/* El servidor confirma el aviso; cerrar una pestaña o perder red no lo consume. */
(() => {
  const csrf = document.currentScript.dataset.csrf;
  let abierto = false;
  window.mostrarAvisosCombustible = async (requestApi, vigente = () => true) => {
    if (abierto) return;
    abierto = true;
    let dialog;
    try {
      const response = await requestApi('/avisos-combustible/');
      if (!response.ok || !vigente()) return;
      const avisos = await response.json();
      for (const aviso of avisos) {
        if (!vigente()) break;
        dialog = document.createElement('dialog');
        dialog.className = 'combustible-aviso';
        dialog.setAttribute('aria-labelledby', 'combustible-aviso-titulo');
        const titulo = document.createElement('h2');
        titulo.id = 'combustible-aviso-titulo';
        titulo.textContent = aviso.titulo;
        const mensaje = document.createElement('p');
        mensaje.textContent = aviso.mensaje;
        const ayuda = document.createElement('p');
        ayuda.textContent = 'Después de pulsar “Entendido”, este aviso no volverá a aparecer.';
        const error = document.createElement('p');
        error.setAttribute('role', 'alert');
        error.className = 'combustible-error';
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = 'Entendido';
        dialog.append(titulo, mensaje, ayuda, error, button);
        dialog.addEventListener('cancel', event => event.preventDefault());
        document.body.append(dialog);
        dialog.showModal();
        await new Promise(resolve => {
          dialog.addEventListener('close', resolve, { once: true });
          button.addEventListener('click', async () => {
            if (button.disabled) return;
            button.disabled = true;
            button.textContent = 'Guardando…';
            error.textContent = '';
            try {
              const saved = await requestApi(`/avisos-combustible/${aviso.id}/confirmar/`, {
                method: 'POST', headers: { 'X-CSRFToken': csrf },
              });
              if (saved.status !== 200 || !(await saved.json()).confirmado) throw new Error('No confirmado');
              dialog.close();
              resolve();
            } catch (_) {
              error.textContent = 'No se pudo guardar la confirmación. Revisa tu conexión e inténtalo de nuevo.';
              button.disabled = false;
              button.textContent = 'Entendido';
            }
          });
        });
        dialog.remove();
      }
    } finally {
      abierto = false;
    }
  };
  if (document.currentScript.dataset.iniciar === 'true') {
    window.mostrarAvisosCombustible((path, options) => fetch(`/api/logistica${path}`, { ...options, cache: 'no-store' })).catch(() => {});
  }
})();

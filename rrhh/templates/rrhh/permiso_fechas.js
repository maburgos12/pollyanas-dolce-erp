function iniciarFechasPermiso(form, nombreTipo) {
  const modalidad = form.elements.modalidad_dias;
  const inicio = form.elements.fecha_inicio;
  const fin = form.elements.fecha_fin;
  const etiquetaInicio = form.querySelector('[data-permiso-inicio]');
  const etiquetaFin = form.querySelector('[data-permiso-fin]');
  const total = form.querySelector('[data-permiso-dias]');

  function actualizar() {
    const completo = form.elements[nombreTipo].value === 'permiso_dia';
    const rango = completo && modalidad.value === 'rango';
    modalidad.disabled = !completo;
    modalidad.parentElement.hidden = !completo;
    for (const campo of [inicio, fin]) {
      const valor = campo.value;
      campo.type = completo ? 'date' : 'datetime-local';
      campo.value = completo ? valor.slice(0, 10) : valor.length === 10 ? valor + 'T00:00' : valor;
    }
    fin.disabled = completo && !rango;
    fin.closest('label').hidden = fin.disabled;
    fin.required = rango;
    fin.min = rango ? inicio.value : '';
    if (fin.disabled) fin.value = '';
    etiquetaInicio.textContent = completo && !rango ? 'Fecha' : 'Desde';
    etiquetaFin.textContent = rango ? 'Hasta' : 'Hasta (opcional)';
    const dias = rango && inicio.value && fin.value >= inicio.value
      ? (Date.parse(fin.value) - Date.parse(inicio.value)) / 86400000 + 1 : 0;
    total.textContent = dias ? `${dias} ${dias === 1 ? 'día calendario' : 'días calendario'}` : '';
  }

  form.addEventListener('change', event => {
    if (event.target.name === nombreTipo || [modalidad, inicio, fin].includes(event.target)) actualizar();
  });
  actualizar();
}

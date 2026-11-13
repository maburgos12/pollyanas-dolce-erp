function iniciarFechasPermiso(form, nombreTipo) {
  const modalidad = form.elements.modalidad_dias;
  const inicio = form.elements.fecha_inicio;
  const fin = form.elements.fecha_fin;
  const etiquetaInicio = form.querySelector('[data-permiso-inicio]');
  const etiquetaFin = form.querySelector('[data-permiso-fin]');
  const total = form.querySelector('[data-permiso-dias]');

  const horas = [form.elements.hora_desde, form.elements.hora_hasta];

  function actualizar() {
    const completo = form.elements[nombreTipo].value === 'permiso_dia';
    const tipo = form.elements[nombreTipo].value;
    const option = modalidad.options;
    for (const item of option) {
      item.hidden = item.disabled = item.value !== 'dia' && item.value !== (completo ? 'rango' : 'laborables');
    }
    if (option[modalidad.selectedIndex]?.disabled) modalidad.value = 'dia';
    const repetir = tipo === 'permiso_hora' && modalidad.value === 'laborables';
    const rango = completo && modalidad.value === 'rango';
    modalidad.disabled = !tipo;
    modalidad.closest('[data-permiso-modalidad]').hidden = !tipo;
    horas[0].closest('[data-hora-repetida]').hidden = !repetir;
    horas.forEach(hora => { hora.required = repetir; hora.disabled = !repetir; });
    for (const campo of [inicio, fin]) {
      const valor = campo.value;
      const hora = horas[campo === fin ? 1 : 0];
      if (repetir && valor.length > 10) hora.value = valor.slice(11, 16);
      campo.type = completo || repetir ? 'date' : 'datetime-local';
      campo.value = completo || repetir ? valor.slice(0, 10) : valor.length === 10 ? valor + 'T' + (hora.value || '00:00') : valor;
    }
    fin.disabled = completo && !rango;
    fin.closest('[data-permiso-final]').hidden = fin.disabled;
    fin.required = rango || repetir;
    fin.min = rango || repetir ? inicio.value : '';
    if (fin.disabled) fin.value = '';
    etiquetaInicio.textContent = repetir ? 'Fecha inicial' : completo && !rango ? 'Fecha' : 'Desde';
    etiquetaFin.textContent = repetir ? 'Fecha final' : rango ? 'Hasta' : 'Hasta (opcional)';
    const dias = rango && inicio.value && fin.value >= inicio.value
      ? (Date.parse(fin.value) - Date.parse(inicio.value)) / 86400000 + 1 : 0;
    total.textContent = dias ? `${dias} ${dias === 1 ? 'día calendario' : 'días calendario'}` : '';
  }

  form.addEventListener('change', event => {
    if (event.target.name === nombreTipo || [modalidad, inicio, fin].includes(event.target)) actualizar();
  });
  actualizar();
  return actualizar;
}

function combinarHorasRepetidas(form, datos) {
  if (datos.modalidad_dias !== 'laborables') return datos;
  datos.fecha_inicio += 'T' + form.elements.hora_desde.value;
  datos.fecha_fin += 'T' + form.elements.hora_hasta.value;
  return datos;
}

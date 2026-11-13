function confirmarPermisoRepetido(resumen) {
  const fechas = lista => lista.map(fecha => fecha.split('-').reverse().join('/')).join(', ');
  const dias = resumen.fechas_laborables;
  const excluidos = resumen.descansos.length ? '\nDescansos excluidos: ' + fechas(resumen.descansos) : '';
  return window.confirm(
    `${dias.length} día(s) laborable(s) · ${resumen.minutos_por_dia} min por día · ${resumen.minutos_total} min en total\n`
    + fechas(dias) + excluidos + '\n¿Confirmas estas fechas?'
  );
}

(() => {
  'use strict';
  const month = document.getElementById('mes_periodo');
  const half = document.getElementById('quincena');
  const start = document.getElementById('fecha_inicio');
  const end = document.getElementById('fecha_fin');
  function selectPeriod() {
    if (!month || !half || !start || !end || !half.value || !/^\d{4}-\d{2}$/.test(month.value)) return;
    const [year, mon] = month.value.split('-').map(Number);
    const last = new Date(Date.UTC(year, mon, 0)).getUTCDate();
    start.value = `${month.value}-${half.value === '1' ? '01' : '16'}`;
    end.value = `${month.value}-${half.value === '1' ? '15' : String(last).padStart(2, '0')}`;
  }
  month?.addEventListener('change', selectPeriod);
  half?.addEventListener('change', selectPeriod);
  [start, end].forEach(input => input?.addEventListener('change', () => { if (half) half.value = ''; }));
  document.getElementById('rrhh-print-button')?.addEventListener('click', () => window.print());
  // Permission folios are expanded in the dedicated print view before printing.
  if (document.body.classList.contains('rrhh-print-page')) document.querySelectorAll('.rrhh-permit-details').forEach(el => { el.open = true; });
})();

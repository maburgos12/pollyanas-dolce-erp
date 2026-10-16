(() => {
  const panels = [...document.querySelectorAll('.mo-panel')];
  const tabs = [...document.querySelectorAll('.mo-tabs a')];
  function showTab(id) {
    const target = document.getElementById(id);
    if (target?.tagName === 'DETAILS') target.open = true;
    id = target?.closest('.mo-panel')?.id || 'productos';
    panels.forEach(panel => { panel.hidden = panel.id !== id; });
    tabs.forEach(tab => { tab.classList.toggle('active', tab.dataset.openTab === id); tab.setAttribute('aria-current', tab.dataset.openTab === id ? 'page' : 'false'); });
  }
  document.querySelectorAll('[data-open-tab]').forEach(link => link.addEventListener('click', () => showTab(link.dataset.openTab)));
  window.addEventListener('hashchange', () => showTab(location.hash.slice(1)));
  showTab(location.hash.slice(1));
  document.querySelectorAll('[data-detail]').forEach(button => button.addEventListener('click', () => {
    document.querySelectorAll('.mo-detail').forEach(panel => { panel.hidden = panel.id !== button.dataset.detail; });
    document.querySelectorAll('[data-detail]').forEach(other => { other.setAttribute('aria-expanded', other === button ? 'true' : 'false'); other.closest('tr').classList.toggle('selected', other === button); });
  }));
  document.querySelector('#mo-search')?.addEventListener('input', event => {
    const query = event.target.value.toLocaleLowerCase().trim();
    let visible = 0;
    document.querySelectorAll('[data-product-row]').forEach(row => { row.hidden = !row.dataset.search.includes(query); if (!row.hidden) visible++; });
    document.querySelector('#mo-no-match').hidden = visible > 0;
  });
})();

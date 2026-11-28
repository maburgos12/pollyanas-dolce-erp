(() => {
  'use strict';
  const print = () => window.print();
  document.getElementById('permiso-imprimir').addEventListener('click', print);
  if (new URLSearchParams(window.location.search).get('imprimir') === '1') {
    window.addEventListener('load', print, { once: true });
  }
})();

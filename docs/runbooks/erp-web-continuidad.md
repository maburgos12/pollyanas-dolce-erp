# Cambio de instancia web del ERP con Nginx

La promoción es una operación separada del despliegue actual de Compose. Se usa
con dos instancias web en puertos locales y versiones compatibles con la misma
base de datos y los mismos estáticos. El PR incorpora el control de tráfico.
La preparación de la instancia Django y la primera instalación de Nginx se
validan en staging antes de activar el procedimiento en el VPS.

## Preparar Nginx y la instancia

1. Preparar una configuración revisada de Nginx. Dentro de `http`, agregar
   `/etc/nginx/conf.d/erp-web-upstream.conf` con
   `upstream erp_web { server 127.0.0.1:8011; }`.
   En el bloque HTTPS del ERP, conservar los encabezados del proxy y configurar
   la ubicación `location /` con estas directivas:

   ```nginx
   proxy_pass http://erp_web;
   proxy_next_upstream error timeout http_502 http_503 http_504;
   proxy_hide_header X-ERP-Backend;
   add_header X-ERP-Backend $upstream_addr;
   ```

   Validar con `nginx -t` y recargar. Confirmar que `/login/` devuelve HTTP 200
   y `X-ERP-Backend: 127.0.0.1:8011` antes de preparar la promoción.
2. Arrancar el candidato en otro puerto de loopback con el commit aprobado,
   una raíz de código separada y almacenamiento compartido para los datos que
   deban persistir. Verificar el commit en el contenedor y ejecutar
   `manage.py check` y `manage.py migrate --check`. Validar las rutas y activos
   afectados en staging. Reservar capacidad para dos instancias simultáneas.
3. Ejecutar como operador del VPS:

   ```bash
   sh scripts/promote_erp_web.sh 8011 8012
   ```

   El comando usa el bloqueo del despliegue del ERP, prueba ambas instancias,
   reemplaza el upstream de forma atómica, valida y recarga Nginx, y verifica
   qué backend respondió. Conserva el anterior como respaldo. Si el cambio
   no se confirma, restaura y comprueba la ruta anterior.

## Operación

- Conservar ambos procesos y sus versiones mientras el anterior sea el respaldo.
  Para volver a preferir el anterior si ambos están sanos, invocar la promoción
  con los puertos invertidos. Si el principal está caído y el respaldo atiende,
  validar y recargar una configuración con el respaldo como único servidor
  antes de retirar la instancia caída.
- La recarga de Nginx conserva solicitudes ya abiertas. El respaldo atiende
  solicitudes cuando el principal no está disponible. Mantener deshabilitada
  la opción `non_idempotent` para evitar reenvíos de escrituras ya iniciadas.
- La prueba `bash scripts/test_promote_erp_web.sh` usa Nginx real y procesos
  HTTP independientes, sin credenciales ni servicios del ERP.
- Mantener coordinado el publicador actual de Compose. Su reinicio de `web`
  sigue siendo un procedimiento distinto. Migraciones incompatibles, cambios
  de workers y cambios de estáticos requieren una estrategia de compatibilidad
  antes de una promoción. No usar dos procesos sobre un checkout que se actualice
  en el lugar.

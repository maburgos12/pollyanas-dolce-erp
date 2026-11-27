# Cargas de combustible sin turno — plan de implementación

**Objetivo:** registrar la nota autorizada sin fabricar un turno, conservando fecha, foto, responsable, litros y autor.
**Arquitectura:** extender CargaCombustibleUnidad; formulario administrativo con permisos existentes, una función de captura y reportes compatibles con NULL. PostgreSQL 16, Django templates y contrato global data-async-action.

- [x] PostgreSQL aislado y fuentes: ejecutar inventario_fuentes_datos --term combustible; verificar ausencia de duplicado y maestros operativos.
- [x] Pruebas de aceptación antes del cambio: ejecutar logistica.tests_combustible_sin_turno; deben fallar por ruta ausente. Cubren precisión, fecha, autor, duplicados, acceso y reportes.
- [x] Modelo/migración: bitacora nullable, litros decimal(8,3), fecha_carga nullable, registrado_por, estación/folio/notas. Fecha consumo = fecha_carga o fecha_registro. No cambiar históricos.
- [x] Captura: formulario ModelForm, sólo permisos de corrección actuales, registrar bajo transacción/bloqueo y audit log; unicidad estación/folio cuando existen; hash y lectura anterior para duplicados. Rechazar repetidos con referencia al registro existente.
- [x] UI: nuevo formulario y sección sin turno en Bitácoras; JSON/HTML comparten guardado, error conserva inputs, éxito muestra recibo, bump de SW.
- [x] Consumidores: corrección y serializer toleran NULL; presupuesto/conciliación usan fecha de consumo; costeo mantiene sin turno sin asignar a recorridos.
- [x] Validar: check, migrate --check, makemigrations --check; pruebas de logística, API y reportes afectados; navegador local real. OCR delegado y diff completo.
- [ ] Entrega: commit sólo alcance propio, PR borrador, CI test del SHA, merge y deploy oficial autorizados por petición de completar registro. Capturar nota una vez y verificar datos, foto, pantalla y total productivos.
- [ ] Cierre: evidencia fuera del repo, retirar entorno sintético propio con helper y cierre oficial de worktree; conservar recursos ajenos.

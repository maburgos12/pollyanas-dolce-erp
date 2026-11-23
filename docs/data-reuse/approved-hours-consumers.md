# Proyección pública y recibos de consumidores — 2026-11-22

Fuente única de intención: SolicitudHorarioEspecial v2 aprobada + bitácora de revisión.
No se crean tablas, maestros, horarios habituales ni equivalencias por semejanza.
Los resolutores conservan precedencia fecha > rango > semanal y reglas legacy conservadoras.
La lectura recorre páginas de 100 solicitudes, con tope seguro de 10 000 históricas
y 100 aplicables por sucursal/fecha; sólo las aplicables consumen este último límite.
Al exceder cualquiera de esos topes queda UNKNOWN.

## Contratos

- ERP `GET /api/public/branch-hours/?branch_codes=MATRIZ,EL_TUNEL&target_date=YYYY-MM-DD`:
  anónimo, sólo negocio público, hasta diez códigos exactos operativos; ayer hasta +366
  días de America/Mazatlan. No consulta horarios habituales por HTTP. No expone ID,
  motivo, comando, actor, auditoría ni secretos. Proyección contiene identidad pública,
  fecha/zona, checked_at, status (VERIFIED/NO_OVERRIDE/UNKNOWN), source, revisión,
  closed_all_day y ventanas. Sin caché HTTP.
- Tienda `GET /api/branch-hours?target_date=...`: consume esa proyección y valida identidad,
  fecha, zona, frescura <=15m, fuente e intervalos. `/api/branches` sigue independiente;
  jamás tienda → ERP → tienda en esta ruta. UI publica hoy/próximos siete días y mantiene
  horario habitual plegado como referencia, sin tocar banderas pickup ni pedidos.
- Maya `GET /branches/hours-status?branch_code=...&target_date=...`: proyección pública
  mínima del consumidor real `open_sales_read_tools().branches.get_operational_status`.
  Usa el catálogo web y credenciales privadas ya existentes de ventas; no consulta
  clientes, conversaciones, LLM ni manda mensajes. No expone `is_open` calculado a mediodía.
  Agrega código SINALOA_LEYVA verificado; no crea registros en catálogo legacy.

## Recibos y límites explícitos

Telegram compara código, nombre, fecha, zona, fuente, revisión, cierre e intervalos;
HTTP 200 no basta. Máximo cuatro pares sucursal/fecha en paralelo por comprobación,
URLs fijas, sin redirects, cuerpos <=64KiB, timeouts 3s conexión/5s lectura y límite
8s entre fragmentos. Guarda recibos dentro del resumen existente, espera 30s entre
lotes y 5m tras confirmación, evidencia <=15m. Revalida revisión central incluso con
caché para impedir Finalizado de una intención sustituida. Google conserva recibos
concurrentes al reconciliar. El bot sondea hasta 20 registros QUEUED por ciclo, priorizando los nunca comprobados
y después los menos recientemente comprobados. Conserva solicitudes futuras sin límite
por antigüedad y termina el seguimiento sólo cuando se cancela o concluye su vigencia.

La comprobación cubre fechas aplicables de los próximos siete días; rangos más largos
u órdenes futuras dicen programado, nunca Finalizado de una cobertura parcial. Recurrencia
semanal sin fecha final permanece programada aunque haya lecturas de la semana actual. Finalizado exige lecturas de
Google, tienda y Maya; no afirma prueba visual de Maps ni todos los días futuros indefinidos.

Preview y botón de aprobación hacen preflight Google GET-only con candidato aún no aprobado,
configuraciones existentes y ownership existente. Reutilizan projection_fields, hasta diez
lecturas, cuatro concurrentes, timeout transporte 5s. Conflictos manuales conocidos impiden
aprobación. Si no hay credenciales o red, aviso explícito de preflight pendiente; la intención
ERP conserva independencia y puede aprobarse sin declarar alineación Google.

## Evidencia y operación pendiente

Pruebas PostgreSQL aisladas usan fixtures; las pruebas de transporte mockean conexiones.
No confundir esa evidencia con producción. Responsable raíz validó ERP Sinaloa id18/código
SINALOA_LEYVA y tienda id31/slug sucursal-sinaloa-de-leyva; vínculo exacto de código en tienda
requiere respaldo y operación puntual documentada en repo tienda. Ninguna bandera cambia.
Mauricio eligió Google publicado como base inicial. No se importó todavía; el bootstrap
y la verificación del corte inicial quedan a cargo del responsable raíz. No se activó el bot ni se desplegó.

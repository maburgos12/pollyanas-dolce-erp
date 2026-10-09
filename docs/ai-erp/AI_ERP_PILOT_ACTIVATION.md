# Activación acotada del piloto: fallas confirmadas

Mauricio autorizó el 9 de octubre de 2026 subir el techo conservador acumulado
del piloto de USD1 a USD5 y habilitar reportes de falla únicamente para su cuenta
existente, PK2. Esta autorización sustituye el techo monetario original de
AI_ERP_PUBLICATION_READ.md; no reinicia ni borra las reservas del piloto.

## Cambio mínimo

El techo se fija en agent_pilot.MAX_USD, por lo que requiere un cambio de código.
Se conservan PILOT_ID, modelo, precios de reserva, bloqueo PostgreSQL, auditoría,
20 turnos, seis ciclos por turno, límites de contexto, herramientas y tiempo.
No hay migraciones, dependencia nueva ni variable adicional para el presupuesto.
La etiqueta de la pantalla pasa de «Piloto READ» a «Piloto»; las tarjetas siguen
mostrando por separado consultas, propuestas y comprobantes del servidor.

La activación operativa establece AI_AGENT_INCIDENTS_ENABLED=true en el VPS y
mantiene AI_AGENT_PILOT_USER_ID=2. READ y activos conservan sus gates existentes.
La admisión de incidentes pasa por can_read_assets/is_pilot_participant; ningún
otro usuario recibe acceso por este cambio, aunque sea superusuario.

El modelo prepara un borrador. El botón autenticado con CSRF, versión y hash es
la única confirmación; el servidor vuelve a validar permisos, estado y recurso.
Se conserva la auditoría atómica y la idempotencia del folio. La confirmación no
consume una llamada al modelo. Sólo fallas de equipos, según el alcance aprobado
en AI_ERP_INCIDENT_CONFIRMATION.md; sin ampliar otras escrituras.

## Validación y reversión

Probar el límite antes de contactar al proveedor, dos conexiones disputando la
última reserva, conservación de reservas anteriores, participantes y regresiones
de incidentes/READ. El presupuesto incluye el historial ya registrado.
Desplegar por Git y deploy_web_safe.sh; activar el flag con recuperación acotada
de la configuración y recrear web para cargar el entorno, porque restart no lo
actualiza. Registrar la huella del ledger antes y después, cupo, usuario, gates,
estado servido y ausencia de escrituras de prueba en producción.

La prueba CREATE real exige que Mauricio indique una falla auténtica y confirme
el borrador. No crear incidencias ficticias ni afirmar validación de creación
real por una flag habilitada. Queda separada de la validación de activación.

Rollback operativo: apagar el flag y recrear web, preservando reportes y reservas.
Revertir el código restaura USD1, pero si las reservas acumuladas exceden ese
techo la admisión se cierra; nunca limpiar el ledger para evitar ese bloqueo.

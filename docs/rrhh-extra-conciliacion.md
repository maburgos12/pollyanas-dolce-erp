# Tiempo extra diario y conciliación

## Diseño aprobado

La jornada diaria de ocho horas incluye los 35 minutos de comida. Se compara la duración entre entrada y salida con la jornada; no se descuenta la comida una segunda vez ni se usa minutos_trabajados, que Hik ya puede haber reducido por comida. Si existen checadas reales de una comida mayor, únicamente el exceso sobre 35 minutos se descuenta. Point no aporta esas marcas reales; sus marcas sintéticas no se usan para descontar comida. Con turno explícito se usa su duración programada; sin turno se usa la jornada de ocho horas ratificada por Dirección. La tolerancia conserva el umbral de 10 minutos del catálogo cuando no hay turno; con turno se respeta su tolerancia. No se infieren horarios para retardos.

Detectado significa duración adicional observada, no autorización ni pago. Sin turno, una entrada 07:56 y salida 18:00 detecta 124 minutos frente a 120 autorizados: la diferencia de 4 minutos queda visible. No se redondea a dos horas para aparentar conciliación.

La conciliación usa empleado y fecha de trabajo, no fecha de captura/autorización. Respeta autorizados y pagados, distingue rechazados y pendientes y no agrega dos veces la solicitud automática y una captura manual para el mismo tiempo. Los registros independientes se mantienen independientes: no se inventa una relación de auditoría con asistencia.

El cálculo/conciliación de reporte es de solo lectura. Los importadores generan o ajustan únicamente el saldo automático pendiente al recibir checadas. Cuando se captura/autoriza/corrige una HoraExtra se actualiza únicamente la conciliación de extra del día afectado, sin re-evaluar faltas, bonos ni nómina. No se ejecuta un backfill productivo.

## Plan de ejecución

- [x] Reproducir fallos en PostgreSQL con comida incluida, sin turno, captura posterior, rechazo, otras fechas/personas y CSV/XLSX.
- [x] Implementar cálculo en minutos y conciliación compartida para importadores, incidencias y reporte.
- [x] Evitar duplicados con bloqueo de asistencia y conservar cantidades/autoría de registros finales.
- [x] Actualizar conciliación ante cambios de HoraExtra, incluyendo eliminación y cambio de fecha/persona.
- [x] Mostrar totales y columnas diarias en el reporte de RRHH y su exportación; incluir días que solo tengan HoraExtra.
- [x] Validar pruebas de RRHH, Hik, Point y consumidores afectados, checks y ausencia de migraciones.
- [ ] Revisar diff, CI de PR borrador, merge, despliegue oficial y lectura real en producción.

## Límites

Sin entrada/salida completas o con intervalo inválido no se acredita cero extra: se presenta el motivo de no cálculo y las autorizaciones existentes. La puntualidad, descansos y asignaciones históricas de horario requieren su propia referencia real y no se habilitan con turnos inferidos. La regla no cambia los valores capturados de nómina ni concede pago automático.

## Validación local

PostgreSQL 16 aislado: 630 pruebas de RRHH, sincronización Point y bonos pasaron; 24 pruebas específicas pasaron después de ampliar cobertura de XLSX, tolerancia, cancelación explícita y autorización superior. `check`, `migrate --check` y `makemigrations --check --dry-run` sin errores ni cambios de esquema. Navegador real con persona ficticia mostró 124 minutos detectados, 120 autorizados y 4 pendientes. CSV/XLSX conservan una sola cantidad diaria aunque haya varias incidencias. En el acceso HTTP local por host.docker.internal el navegador informa COOP por origen no confiable; no hubo errores de JavaScript del reporte. La validación productiva se realiza tras el despliegue.

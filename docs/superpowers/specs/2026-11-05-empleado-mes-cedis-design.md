# Empleado del mes CEDIS

Borrador funcional aprobado por Mauricio en este hilo. Alcance: Producción y Administración; Ventas excluido.

## Flujo aprobado
RRHH abre mes y fecha límite; cada jefatura postula dos colaboradores de su equipo canónico. Cada evaluación tiene 14 criterios de 1 a 5, tres bloques 5/5/4 y máximo 70. Se conserva reconocimiento especial, recomendación sí/no, justificación, evaluador y fecha. Borradores incompletos permitidos; envío exige todos los criterios. RRHH compara cuatro evaluaciones enviadas, devuelve con motivo o elige un ganador con justificación (incluye empate o menor puntuación). La elección no es automática. RRHH aprueba un premio efectivo/vales/descanso y registra entrega, responsable y comprobante o acuse. DG supervisa en lectura.

## Fuentes y límites
Empleado es la identidad única; jefe_directo y usuario_erp determinan el equipo. Los evaluadores de Producción y Administración se congelan al abrir periodo, a partir de jefaturas activas únicas. Se guardan snapshots de identidad y puesto al postular. No se modifica el organigrama, nómina, vacaciones, bonos ni datos de asistencia. Las incidencias son evidencia consultable, nunca puntuación automática. No hay comunicaciones externas ni publicación de fotografías. Premio no requiere elegirse al abrir el mes.

## Persistencia y seguridad
Dos modelos nuevos: PeriodoEmpleadoMes y EvaluacionEmpleadoMes, con mes único y candidato único por periodo. Reutilizar AuditLog para antes/después de cada acción. Bloqueo de periodo para serializar postulaciones, selección y entrega; versión de formulario para rechazar sobrescrituras. Evaluaciones enviadas se bloquean hasta devolución explícita. Periodo elegido se congela salvo premio y entrega. Sólo RRHH activo con permiso de gestión administra; DG consulta; evaluador sólo su equipo y sus evaluaciones. Autorización en backend y navegación. Sin autoevaluación ni candidatos jefatura/dirección.

## Experiencia
Página mensual con pendientes y comparativo; formulario en tres secciones con etiquetas y errores accesibles; histórico consultable. Reutilizar base, botones, tokens y navegación compartida del ERP. Evidencia de entrega es referencia/acuse textual; no introducir almacenamiento de archivos en esta etapa.

## Validación
PostgreSQL16 aislado, pruebas de flujo, acceso cruzado, ventas, versiones, duplicados, fechas, congelación e historial. Validación real en navegador escritorio/móvil con datos ficticios locales. Check/migraciones, CI, PR, merge, deploy oficial, GET autenticado de producción sin crear periodos de prueba reales.

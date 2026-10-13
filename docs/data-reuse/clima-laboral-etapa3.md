# Ficha de fuentes — cobertura de contacto para clima laboral

Fecha y ambientes: 2026-10-12, PostgreSQL local aislado y producción en consulta agregada de solo lectura.

## Necesidad y unidad de análisis

Mostrar a RR. HH. qué participantes pendientes tienen un correo o teléfono personal con formato utilizable, cuáles comparten un dato con otro participante y cuáles ya no están activos. La unidad es la persona del padrón o de una campaña. Es una evaluación actual; no confirma titularidad ni autorización para contactar.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Empleado y contacto personal | `rrhh.Empleado` / `rrhh_empleado` | Expediente de Capital Humano | PK de empleado | Producción: 68 activos elegibles; 38 con correo válido, 38 con teléfono válido, 37 con ambos, 29 sin ninguno | RR. HH. y selección de clima |
| Participante y estado | `rrhh.ParticipanteClima` / `rrhh_participanteclima` | Preparación de campaña y respuesta | Campaña + empleado únicos | Producción: 0 campañas y 0 participantes | Detalle de clima y acceso individual |
| Contacto de trabajo | `auth.User.email`, `core.UserProfile.telefono` | Cuenta de ERP y perfil operativo | Usuario de ERP | Fuente distinta documentada en `core/contactos.py`; no consultada para esta cobertura | Avisos operativos |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Contacto personal / contacto de trabajo | Distinta | `core/contactos.py` separa expediente de RR. HH. y cuenta operativa; no se sustituyen | Antes de escoger canal de entrega |
| Mismo correo o teléfono entre empleados | Candidata de contacto compartido, no identidad compartida | Coincidencia de dato normalizado; no prueba que sea la misma persona | RR. HH. antes de distribuir ligas |

## Decisión de diseño

Reutilizar `Empleado.email`, `Empleado.telefono`, `Empleado.activo` y `ParticipanteClima.respondio`. Calcular la cobertura al consultar, sin tabla nueva ni copia de contactos en la respuesta anónima. Mostrar estados y conteos a usuarios con permiso RR. HH.; no enviar mensajes ni habilitar recepción.

Procedimiento reproducible: `python3 manage.py inventario_fuentes_datos --term empleado --term contacto --term correo --term telefono --limit 5 --presence` con PostgreSQL local; consulta agregada de solo lectura en producción de empleados activos de Pollyana y conteo de campañas/participantes. El inventario informa candidatos léxicos, no equivalencias.

Riesgos y pendientes: formato y ausencia de duplicados no prueban titularidad, acceso privado ni consentimiento. Antes de pruebas reales, Mauricio debe aprobar las personas concretas; antes de diciembre se debe elegir canal y autorizar cualquier envío.

# Ficha de fuentes — Clima laboral, etapa 1

Fecha y ambiente consultado: 12 de octubre de 2026, PostgreSQL local aislado para esquema; producción consultada en solo lectura el 7 de octubre de 2026 para cobertura agregada.

## Necesidad y unidad de análisis

Una campaña conserva el cuestionario acordado. Un participante representa un empleado elegible dentro de una campaña y guarda una foto de su área y sucursal. En esta etapa se preparan ligas firmadas y QR individuales que vencen al cierre; todavía no se capturan respuestas ni se envían invitaciones.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Persona elegible | `rrhh.Empleado` / `rrhh_empleado` | RR. HH. | `id` o `codigo`; `activo`, `tipo_personal` | Producción: 97 expedientes, 68 activos al 7-oct; inventario local confirmó tabla y claves | Nómina, bonos, asistencia y otros módulos RR. HH. |
| Área | `Empleado.departamento` | RR. HH. | Valor del catálogo departamental | Columna presente; se copia como foto de campaña | Organización y reportes RR. HH. |
| Sucursal | `Empleado.sucursal_ref` → `core.Sucursal` | RR. HH. y catálogo de sucursales | FK canónica; texto legado solo para mostrar | 68 activos con `sucursal_ref` al 7-oct | RR. HH. y módulos por sucursal |
| Contacto personal | `Empleado.email`, `Empleado.telefono` | RR. HH. | Expediente del empleado; no identidad verificada por formato | 39 de 68 activos con alguno de formato utilizable al 7-oct | RR. HH.; no se copia a campaña |
| Contacto de trabajo | `auth.User.email`, `core.UserProfile.telefono` | Cuenta ERP | Usuario y perfil operativo | 12 activos con alguno de formato utilizable al 7-oct | Avisos operativos; no se copia a campaña |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Empleado y cuenta ERP | Confirmada solo cuando `Empleado.usuario_erp_id` existe | Relación uno a uno explícita; no se empareja por nombre o contacto | Ninguna para padrón |
| Correo/teléfono personal y de trabajo | Distinta | Fuentes y titularidad operativa diferentes | Verificar contacto antes de cualquier envío |
| Departamento actual y área de campaña | Distinta en el tiempo | Se conserva foto para estabilidad histórica | RR. HH. revisa errores antes de crear campaña |

## Decisión de diseño

Reutilizar `Empleado` y `Sucursal` para seleccionar personas; crear solo campaña y participación de campaña. El cuestionario público se copia como foto versionada en la campaña. Una restricción única por campaña y empleado evita duplicar participantes. No se crean respuestas ni canales de contacto en esta etapa.

Consultas o procedimiento reproducible: `python3 manage.py inventario_fuentes_datos --term empleado --term encuesta --limit 3 --presence` con PostgreSQL local. Los conteos productivos fueron agregados, sin extraer datos personales.

Riesgos y pendientes: la cobertura de contacto no prueba posesión ni entrega; el padrón productivo puede cambiar antes de abrir campaña; falta validar con RR. HH. la elegibilidad final y la clasificación de preguntas del informe anterior.

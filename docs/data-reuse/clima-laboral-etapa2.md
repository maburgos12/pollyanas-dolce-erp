# Ficha de fuentes — clima laboral, respuestas

Fecha y ambiente consultado: 2026-10-12; PostgreSQL local aislado `erp_rrhh_clima_e2`.

## Necesidad y unidad de análisis

Una fila de `ParticipanteClima` representa una persona convocable por campaña y su estado de participación. Una fila nueva de `RespuestaClima` representa un formulario anónimo; no conserva identificador de persona, liga ni hora individual. Los resultados por pregunta se calculan sobre estas respuestas.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Persona | `rrhh.Empleado` | RR. HH. | PK empleado | Padrón revisado en etapa 1; base local nueva sin registros | Campaña, otros módulos RR. HH. |
| Campaña | `rrhh.CampanaClima` | Preparación interna | PK campaña | Inventario local: tabla presente, sin registros | Lista y detalle de clima |
| Participación | `rrhh.ParticipanteClima` | Preparación y envío del formulario | Única campaña + empleado | Inventario local: tabla presente, sin registros | Detalle y acceso individual |
| Respuesta de higiene | `operacion.RespuestaHigiene` | Registro operativo de higiene | Registro + punto | Candidato léxico del inventario local, sin registros | Operación; dominio distinto |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Respuesta de higiene / clima | Distinta | Diferente propósito, preguntas y unidad de análisis | No |
| Participante / respuesta | Distinta | La participación identifica a una persona; la respuesta debe evitar esa relación | No |

## Decisión de diseño

Extender campaña y participante existentes. Crear solo `RespuestaClima` separada para las respuestas y usar el cuestionario ya guardado en la campaña. No duplicar el padrón ni la captura de empleados. La campaña permanece cerrada por defecto y no hay acción de envío o apertura en esta etapa.

Consultas reproducibles: `python3 manage.py inventario_fuentes_datos --term clima --term encuesta --term respuesta --limit 3 --presence` con PostgreSQL local. El inventario solo aporta candidatos léxicos.

Riesgos y pendientes: texto libre puede revelar identidad por su contenido; se guarda pero no se publica en el panel. La activación y entrega de ligas requieren revisión antes de diciembre. El panel solo muestra agregados con al menos cinco respuestas y omite el desglose por área cuando algún área tiene menos de cinco.

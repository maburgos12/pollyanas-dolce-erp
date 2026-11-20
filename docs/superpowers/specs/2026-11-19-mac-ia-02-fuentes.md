# Ficha de fuentes — integración automática de Mac-IA-02

Fecha y ambiente: producción, lectura PostgreSQL verificada; corte consultado
2026-11-20 00:17 UTC / 19 de noviembre en America/Mazatlan.

## Necesidad y unidad de análisis

Un trabajo local de resumen de seguimiento sobre un lote acotado de casos ya
registrados. Caso operativo, tarea generada, corrida y comentario de IA son
unidades distintas. No se crea un segundo inventario ni cierre financiero.

## Fuentes candidatas

| Concepto | Fuente | Quién la crea/actualiza | Identificador | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Tareas de seguimiento | orquestacion.AgentTask | rule_runners y runtime existente | id, run_id, task_type; ajuste_id/plan_id en payload según writer | 564 filas guardia/6 ajuste_id en 7 días, sin clave ausente; otros cuatro tipos de cadena con 7 tareas cada uno | Panel Orquestación y runtime |
| Corridas | orquestacion.OrchestrationRun | runners existentes | run_key único, id y rule_id | 225 corridas de cuatro reglas en 7 días; todas registradas success | Panel, auditoría, AgentTask |
| Reglas | orquestacion.OrchestrationRule | catálogo y administración | code único | 13 reglas activas en catálogo; cuatro con corridas observadas | Scheduler y runners; activo no prueba runner |
| Programación | django_celery_beat.PeriodicTask | setup_celery_schedules y configuración existente | name único, task y regla en kwargs | Cuatro programaciones de Orquestación habilitadas | Celery Beat |
| Ajuste de inventario | inventario.AjusteInventario | flujo operativo autorizado | ajuste.id | Writer de la guardia copia ajuste_id; agrupación actual confirmó 6 IDs | Orquestación, inventario y aprobaciones |
| Plan de producción | recetas.PlanProduccion | flujo operativo del plan | plan.id | Writer de cadena referencia plan_id y separa roles | Producción, compras y dirección |
| Modelo local | servicio privado Mac-IA-02 | configuración autorizada del nodo | alias pastelerias-qwen3-8b; revisión/hash en runbook | Salud SSH comprobada y validación previa de tres extracciones y chat | Nuevo ejecutor técnico |

## Alias y equivalencias

| Relación | Estado | Evidencia / límite |
| --- | --- | --- |
| Tareas guardia con mismo ajuste_id → mismo ajuste | Confirmada para agrupar referencias | Mismo writer y PK operativa copiada al JSON; 564 filas/6 IDs. AgentTask no tiene FK formal al ajuste. No fusionar ni cerrar tareas originales. |
| Distintos roles del mismo plan → misma acción | Distinta | La cadena crea demanda, producción, compras y dirección por separado. |
| Tarea pendiente → ajuste sin autorizar | No resuelta | El estado de seguimiento no prueba el estado operativo vigente. |
| Corrida success → caso cerrado | Distinta | En producción hay corridas success que dejan tareas pending. |
| Token local → token pagado evitado | No resuelta | No existe aún comparación de facturación atribuida a este flujo. |

## Decisión de diseño

Reutilizar OrchestrationRun/AgentTask y el panel de Orquestación para la cola e
historial técnico. Extender con un tipo de trabajo local, metadatos de ejecución
y una proyección del estado. No crear tablas maestras ni reemplazar servicios de
inventario, producción, pagos o autorizaciones. El modelo sólo produce comentarios.

## Procedimiento y límites

Se ejecutó inventario_fuentes_datos --term orquestacion --term agente --term tarea
sobre PostgreSQL de producción, sin leer valores personales. Después se consultaron
cuentas por regla/tipo/estado y agrupación por input_payload.ajuste_id dentro de
transacciones SET TRANSACTION READ ONLY. Se leyeron writers y consumidores del
repositorio; el grafo disponible no devolvió símbolos de orquestacion, por lo que
se usó lectura acotada de los archivos referidos por la documentación.

No se activó ningún schedule ni se cambiaron registros. No se consultaron nómina,
bancos, CFDI, credenciales o contenidos de conversaciones. Los comentarios de
arquitectura de abril son históricos y no sustituyen la verificación actual.

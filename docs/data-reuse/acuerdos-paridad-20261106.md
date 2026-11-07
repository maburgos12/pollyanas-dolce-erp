# Ficha de fuentes — paridad de acuerdos ERP/app

Fecha y ambiente: 6 de noviembre de 2026; código y PostgreSQL de producción, solo lectura.

## Necesidad y unidad de análisis

Minuta identificada en Agente DG; proyección de seguimiento en ERP; punto de checklist; evento de comunicación por acuerdo, revisión, destinatario y canal. No son la misma unidad que una lectura o aceptación del proveedor.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
|---|---|---|---|---|---|
| Acuerdo | MinuteAgreement / minute_agreements | API app; ERP la llama por HTTP | id de origen | #103 y #104 verificados en ambas bases | app, ERP, comunicaciones |
| Espejo | SeguimientoItem / seguimiento_seguimientoitem | Importador y webhook firmado; revisión local ERP | source + source_table + source_id; pk ERP distinto | 88 minutas, 88 IDs de origen distintos en consulta | listado personal, panel DG, calendario |
| Punto | checklist_items_json y SeguimientoChecklistItem | API app y writeback ERP | ID del punto en su minuta, no texto similar | #104/844: un punto verificado tras corrección | checklist, entrega, revisión |
| Destinatario | users de app; auth.User y rrhh.Empleado ERP | Cada sistema conserva su identidad | user_id app vs responsable_user_id ERP | #103 usuario app 3 / ERP 4; #104 app 2 / ERP 7 | asignación, acceso, envío |
| WhatsApp | MinuteWhatsAppEvent / minute_whatsapp_events | despacho y webhook de proveedor | minute_id + message_id + evento | #104 con estado read posterior | historial, reintento, pantalla |
| Correo y auditoría | audit_log, flags de minute_agreements; proveedor configurado | Servicio app | evento y destinatario; indicador global insuficiente | #104 evento SEND posterior con sent_to_collaborator aún false | soporte y comunicación |
| Aviso interno | core.Notificacion; notifications app | Flujos de cada sistema | destinatario y entidad/evento | cero avisos vinculados a las cinco minutas iniciales al auditar | bandejas internas |

## Alias y equivalencias

| Identificadores | Estado | Evidencia | Revisión |
|---|---|---|---|
| minute_agreements:103 ↔ ERP 843 | Confirmada | metadata explícita y responsable cotejado | Sin nueva equivalencia |
| minute_agreements:104 ↔ ERP 844 | Confirmada | metadata explícita y contenido verificado | Sin nueva equivalencia |
| Minuta, acuerdo, SeguimientoItem | Dependiente del tipo | Solo tipo MINUTA y source_table minute_agreements | No generalizar a proyectos/compromisos |
| Enviado, entregado, leído | Distintas | flags, aceptación y callbacks expresan hechos diferentes | Mantener estados separados |
| core.Notificacion ↔ WhatsApp/correo | Distintas | Registro interno no demuestra envío externo | No fusionar evidencias |

## Decisión

Reutilizar identidad de origen, importador, webhook, checklist e historial existentes. Extender contrato común y seguimiento por canal. La ausencia de un registro de idempotencia atómica genérico exige revisar persistencias antes de proponer una tabla; no se declara resuelta mediante flags o JSON sin garantías de concurrencia.

Procedimiento: inventario_fuentes_datos ejecutado con Django configurado en PostgreSQL conectado, términos acuerdo, minuta, notificacion y seguimiento. Acuerdo y minuta no dieron candidatos léxicos; seguimiento produjo 11 y notificacion 3. Ese resultado no demuestra ausencia de fuentes: se cotejaron los modelos y filas conocidos con consultas BEGIN READ ONLY limitadas al dominio. Se verificaron código desplegado, rutas y contratos.

Riesgos: diferencias entre repo release y carpeta activa de app todavía requieren cotejo; entregabilidad actual de proveedores no se probó; visibilidad reportada por Johana no se ha validado con su sesión. No se crearon equivalencias ni se modificaron datos en esta fase.

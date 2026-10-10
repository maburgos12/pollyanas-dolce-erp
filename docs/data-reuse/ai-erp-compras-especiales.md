# Ficha de fuentes — compras especiales desde Agente ERP

Fecha y ambiente consultado: 2026-10-10, PostgreSQL de producción en solo lectura; código main 9d64b623.

## Necesidad y unidad de análisis

Una solicitud departamental extraordinaria para pruebas de Producción, dos artículos, cotizaciones por vendedor, compra externa ya realizada y una captura de confirmación. Conversación técnica pendiente distinta de transacción financiera y recepción física.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Solicitud especial | compras.SolicitudCompraDepartamental | departamental_nueva / departamental_enviar | PK, folio SCD, área, solicitante y periodo | Carolina tiene solicitudes Sep/Oct; sus 18 artículos revisados no incluyen productos del caso | Bandeja, detalle, resúmenes |
| Artículo | compras.ItemCompraDepartamental | solicitud y servicios departamentales | PK/solicitud; cantidad/unidad; estimado admite NULL | Ninguna coincidencia glicerina/invertido/Trimoline/Dayman | Cotizaciones, compromisos, órdenes y recepción |
| Cotización | compras.CotizacionCompraDepartamental | formulario y selección controlada | PK/versión, proveedor, plataforma; envío por artículo/cotización | Fuente existente con registros; vendedores REGGIZ/AChocolart sin correspondencia verificada | Evaluación presupuesto, órdenes, compra |
| Compra realizada | compras.CompraRealizadaDepartamental / IntentoCompraDepartamental | registrar_compra_realizada | PK/intento único, cotización y versión | Exige autorización anterior y comprobante; no se escribió compra para el caso | Compromisos, historial, avisos |
| Evidencia | documento/comprobante existentes | validaciones_archivos y flujo de compras | archivo de compra o cotización | Captura de usuario confirma compra y jueves; no muestra importe/desglose | Detalle y visualizador |
| Proveedor | maestros.Proveedor | alta explícita de vendedor | PK, activo | Solo candidato MERCADO LIBRE #97 entre términos buscados | Cotización y órdenes |
| Conversación técnica | orquestacion.ChatToolCall | runtime y confirmación fuera del modelo | UUID, usuario/conversación, versión, hash | Mecanismo existente de propuestas; dominio compras aún no registrado | Historial, procesos, tarjetas y recibos |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Solicitud especial / extraordinaria | Candidata | Tipo EXTRAORDINARIA ya existe; justificación: pruebas fuera del ciclo | Confirmación al presentar propuesta |
| Mercado Libre / REGGIZ / AChocolart | Distinta o no resuelta | Plataforma y vendedores separados; no fusionar con #97 | Identidad de cada vendedor por artículo |
| Jueves próximo / 2026-10-15 | Candidata | Fecha del contexto 10 oct y calendario; pantalla solo dice jueves | Mantener fecha estimada y confirmar antes de ejecución |
| Compra realizada / recepción / gasto contable | Distintas | COMPRA_REALIZADA no equivale a RECIBIDO_CONFORME ni llena monto_gastado | Respetar sus fuentes y transiciones |

## Decisión de diseño

Reutilizar los modelos, permisos, servicios, presupuesto, historial y comprobantes de Compras Departamentales. Añadir capacidades al mismo Agent Core; conservar propuestas estructuradas en ChatToolCall antes de confirmar. No crear una segunda tabla maestra de solicitudes, no usar herramientas antiguas de SolicitudCompra de insumos como sustituto.

Consulta reproducible: inventario_fuentes_datos con términos compra departamental, cotizacion y comprobante, presence=True y limit=8 (18 candidatos). Consultas ORM acotadas en PostgreSQL: descripción por cuatro términos, mismo filtro en insumos, 18 artículos recientes de Carolina Sep/Oct y proveedores por nombres visibles. Sin escrituras.

Riesgos y pendientes: $129 de envío global sin asignación; dos vendedores no identificados por artículo; compra ya realizada fuera del orden del flujo actual; enlaces de producto obligatorios; solicitante Carolina y actor Mauricio deben quedar separados sin suplantar sesión. Las coincidencias léxicas no prueban inexistencia universal. Falta habilitar herramientas del dominio y modalidades/canales; no se modificaron producción ni permisos.


## Corte implementado

- Captura compartida `compras/services_solicitudes.py`, reutilizada por formulario y agente.
- Solicitudes extraordinarias enviadas sólo al confirmar versión y hash, con solicitante distinto del actor exclusivamente para DG y responsable del área.
- `ChatToolCall` conserva contexto estructurado y evidencia opcional; no se crea otra tabla maestra.
- `CompraRealizadaDepartamental.comprobante` pasa a `blank=True`; migración de estado sin SQL físico ni modificación de registros.
- Compra ya realizada pero sin reparto de envío se conserva como información reportada en evento y ficha, pendiente de regularización. No se asigna proveedor, cotización, compromiso, pago ni recepción.

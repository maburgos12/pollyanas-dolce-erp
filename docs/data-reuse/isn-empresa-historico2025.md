# Ficha de fuentes — ISN de empresa 2025

Fecha: 2026-11-05. Producción PostgreSQL, lectura acotada; pruebas en PostgreSQL16 exclusivo. Autorización de Dirección: adaptar conciliación de ISN exclusivamente para 2025 a nivel empresa, sin sucursales ni cambios al cálculo2026.

## Necesidad y unidad de análisis

Un expediente fiscal por mes de 2025, sin distribución. Comparar el concepto de ISN del comprobante estatal con la base reconstruida de las percepciones reales, respetando ajustes conciliados y prestaciones extraordinarias. No convertir la conciliación fiscal en certificación bancaria ni en cierre de rentabilidad.

## Fuentes candidatas

| Concepto | Modelo | Fuente que crea/actualiza | Identificador | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Nómina real | NominaPeriodo/Linea/ConceptoLinea | Importación histórica conciliada | Periodo y empleado; UUID en observaciones | 75 periodos,1764 líneas,4010 conceptos de percepción de2025; todos HISTORICA | Planeación y rentabilidad |
| Linaje y ajustes | NominaImportacion | Importación histórica | Periodo,archivo_hash,uuids,ajustes_por_uuid | 1764 UUID; ajustes originales se conservan | Conciliación fiscal |
| Contraste fiscal individual | CfdiDescargado | SAT | UUID | 1764 CFDI enlazados; no son otra nómina sumable | Base fiscal y límites de previsión social |
| Impuesto estatal | CfdiDescargado | SAT | UUID y periodo del concepto | 12 comprobantes,ISN total172050.00 | ExpedienteISN |
| Control fiscal | ExpedienteISN | Servicio de conciliación | UUID único y revisión/mes | Cero expedientes2025 al iniciar | Planeación,resultado histórico,admin de consulta |
| Distribución | DistribucionISNEmpleado | Servicio corriente | Expediente y empleado | Exige sucursal; no corresponde al histórico sin adscripción | Costeo por área/sucursal |

Inventario: `inventario_fuentes_datos --term ISN --term nomina` en PostgreSQL configurado; grafo de modelos y llamadas de calcular_isn_sinaloa; inspección de lectores y comando materializar_isn.

## Alias, equivalencias y fuentes oficiales

Se reutilizan códigos CONTPAQi del original y tipos SAT del mismo UUID. Se verifica cada concepto real contra el CFDI más los ajustes documentados en la importación; solo el código17 de ajuste en sueldos puede añadirse como ajuste explícito sin concepto original. No se enlazan personas por nombre ni se usa su puesto/sucursal actual. Ajustes negativos se netean dentro de su mismo concepto; un saldo negativo se rechaza.

Tarifa y exenciones: [Ley de Hacienda de Sinaloa, arts.18 y22, versión con última reforma17-jun-2024](https://retys.sinaloa.gob.mx/Content/Archivos/024017022025Ley%20de%20Hacienda%20del%20Estado%20de%20Sinaloa.pdf). PTU25, primas vacacionales20/22 e indemnizaciones26/29 se excluyen cotejando tipo fiscal. El límite anual de aguinaldo se acumula por persona sin distribuir el impuesto. Previsión social31/32 se limita al importe exento documentado en el CFDI y al importe real del concepto. No se convierte toda percepción exenta de ISR en exenta de ISN: horas extra y prima dominical siguen en la base.

UMA enero2025:108.57, [DOF2024](https://sidof.segob.gob.mx/notas/docFuente/5714085). Febrero–diciembre2025:113.14, [DOF2025](https://sidof.segob.gob.mx/notas/docFuente/5746930). El servicio corriente de2026 conserva sus vigencias y contratos.

## Decisión y contratos

Reutilizar ExpedienteISN y su metadata, sin migración ni tabla nueva. Servicio separado `services_isn_historico`; comando existente `materializar_isn --empresa-2025`. Preview por defecto. Aplicación revalida fuentes bloqueadas y huella, es idempotente para la misma evidencia y rechaza reemplazar un expediente distinto. No permite aceptar diferencias ni cambiar política a mano. Los meses que difieren más que la tolerancia existente de1peso se guardan DISCREPANCIA, sin aplicado_en. No se crean DistribucionISNEmpleado ni se actualizan registros de nómina.

El lector de planeación reconoce una sola vez el expediente APLICADO; las discrepancias2025 quedan visibles y su CFDI sigue marcado no conciliado, por lo que resultado histórico lo excluye. Costeo de cargas que requiere detalle de sucursal no toma un expediente corporativo sin distribuciones. La ruta corriente de2026 y el cálculo de su base no cambian.

## Pruebas y límites

Pruebas de origen histórico, cobertura anual, duplicados y solapamientos, ajustes respaldados, conceptos desconocidos, límites de previsión/aguinaldo, discrepancia, comando en seco, idempotencia, fuentes modificadas y protección de2026. Contraste local con copia de fuentes reales —no aplicación productiva— produce nueve meses dentro de tolerancia; febrero -6.49,julio14.98,noviembre1140.00 pendientes. Esas diferencias no se aceptan automáticamente ni justifican modificar la nómina.

`importe_pagado` es el nombre legado del campo que recibe el concepto estatal; la metadata declara explícitamente `pago_bancario=NO_CERTIFICADO`. El expediente histórico certifica el contraste fiscal, no transferencia o liquidación bancaria. No se modifica el estado QF20 ni se declara utilidad final.

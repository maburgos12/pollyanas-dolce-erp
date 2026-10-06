# Ficha de fuentes — evidencias históricas en NAS

Fecha y ambiente: 2026-10-06; producción consultada exclusivamente en lectura, pruebas con PostgreSQL 16 vacío y registros sintéticos.

## Necesidad y unidad de análisis

Conservar la visualización de fotos históricas al retirarlas del disco VPS. Unidad: nombre relativo de un archivo, todas sus referencias y sus bytes. No es una segunda captura de bitácoras ni una nueva fuente empresarial.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Salida, llegada y ticket | `logistica.BitacoraSalidaLlegada` / `logistica_bitacorasalidallegada` | Captura de logística, web/PWA | PK, fecha, repartidor, unidad; tres FileFields | Mayo 2026: 96 registros cerrados, 202 nombres distintos, 511458820 bytes, ninguno ausente | Bitácoras, capturas, unidades, revisiones de combustible |
| Ticket de carga | `logistica.CargaCombustibleUnidad` | Registro de combustible | PK, `foto_ticket`, unidad | Ningún nombre del lote de mayo compartido con esta fuente | OCR y auditoría de combustible |
| Archivos operativos | `MEDIA_ROOT` | Storage existente | Nombre relativo; nombres sin cambios | Lectura y tamaño reales de los 202 originales | Django FileField, imágenes del ERP |
| Respaldo NAS | QNAP, `ERP_BACKUPS` | HBS diario desde exportación VPS | Destino remoto independiente del staging | Ruta de backups existente; NO demuestra recepción de fotos de este piloto | Recuperación de respaldos |

## Alias y equivalencias

| Términos | Estado | Evidencia | Revisión |
| --- | --- | --- | --- |
| Foto de tablero / salida / llegada | Confirmada por FileField específico | `foto_tablero_salida`, `foto_tablero_llegada` | No unir campos ni registros |
| Ticket bitácora / ticket carga | Fuentes distintas | Comparten prefijo de carpeta; no implican identidad | Excluir cualquier nombre compartido fuera del lote |
| Publicado en staging / recibido NAS | Distintos | Staging puede existir aunque NAS esté desconectado | Exigir montaje remoto identificado y SHA de los bytes |

## Decisión

Extender FileSystemStorage y la ruta de medios existente con fallback para archivos indexados y verificados. Reutilizar permisos, registros y nombres actuales. Índice privado de ubicación/integridad por archivo; no modelos, migraciones, nueva captura ni cambio de FileFields.

Revisión reproducible: `inventario_fuentes_datos --term bitacora` y sinónimos ticket, combustible, evidencia; consultas acotadas al mes sobre los modelos candidatos. El inventario léxico se contrastó con registros y archivos reales. El comando de archivo vuelve a buscar referencias en todos los FileFields y excluye referencias fuera del lote.

Pendientes: cuenta técnica limitada, montaje privado de lectura, recepción real y verificación autenticada de todas las fotos antes del retiro. El NAS no constituye por sí solo una segunda copia independiente: conservar respaldo separado.

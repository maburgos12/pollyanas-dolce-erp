# Regularización histórica IMSS/SIPARE y cruce temporal

## Resultado

Planeación de personal debe mostrar los controles patronales de enero a agosto de 2026 únicamente cuando exista un expediente aplicado y conciliado. Los comprobantes de pago SIPARE se conservan como evidencia distinta de las emisiones EMA/EBA. Un NSS puede cruzar con una persona dada de baja después de haber trabajado en el periodo, sin reactivar su expediente de RRHH. El ISN de agosto y una persona sin identidad inequívoca permanecen pendientes.

## Límites de seguridad

- Mantener la huella SHA-256 y los importes originales de las líneas presupuestales históricas. Ningún monto manual se sobrescribe.
- Una cédula sin encabezado solo se recupera si sus columnas conocidas, sumas de detalle, resumen SUA, comprobante SIPARE y control presupuestal coinciden. Registrar las huellas del XLS y del resumen.
- El comprobante SIPARE combinado se identifica por contenido, registro patronal y periodo mensual, con bimestre opcional; no se clasifica como EMA/EBA. Una copia se almacena con el expediente mensual. El expediente bimestral referencia esa misma evidencia mediante metadatos auditables para evitar duplicar el archivo.
- El cruce por NSS requiere coincidencia única y superposición del periodo con una vigencia laboral documentada. Las identidades ambiguas o sin vigencia quedan sin cruce. No se crean ni alteran empleados ni bajas.
- Los expedientes ya aplicados se reconcilian en una operación específica con previsualización, bloqueo, auditoría y verificación de totales; reenviar el mismo XLS no los recalcula.

## Comprobación

Pruebas rojas/verdes para PDF SIPARE, recuperación acotada de febrero, cruces de exempleados, idempotencia y rechazo ante controles discordantes. Antes de aplicar: dry-run productivo y cotejo de cada importe contra los archivos y líneas existentes. Después: lectura nueva de expedientes, detalles y Planeación. Si falta evidencia o hay discrepancia, el periodo queda pendiente y no se presenta como conciliado.

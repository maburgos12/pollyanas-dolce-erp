# OCR como segunda lectura selectiva

Usar [erp-code-review](../../.agents/skills/erp-code-review/SKILL.md) para cambios
con riesgo material de reasignación/cálculo de datos, concurrencia, colas o contratos
entre integraciones y consumidores. También ante petición explícita de Mauricio.
Para cambios triviales o sólo documentales basta la revisión habitual. Una auditoría
de seguridad integral sigue su flujo específico; esta herramienta no la sustituye.

## Alcance

Se adopta **modo delegado**: OCR produce selección y reglas; Codex revisa con su
autenticación y modelo disponibles. No es un segundo modelo independiente ni activa
el motor nativo de OCR. No se configura una API de modelos adicional. La revisión
no autoriza correcciones, escrituras de datos, mensajes, merge o producción.

La configuración [rule.json](../../.opencodereview/rule.json) incluye pruebas,
documentación y contratos habituales y combina criterios del ERP con reglas del
lenguaje mediante `merge_system_rule: true`. `include` permite pasar filtros por
defecto; **no es una lista cerrada**. Inspeccionar siempre preview, en particular
borrados, archivos grandes y extensiones no soportadas. Conservar filtros de
secretos, binarios y dependencias generadas que no sean objeto de la revisión.

## Herramienta fijada

[tool.json](../../.opencodereview/tool.json) fija v1.14.3, commit y SHA256 del binario
macOS ARM64 validado. Ubicación local:
`$HOME/.local/share/open-code-review/v1.14.3/ocr`. No agregarlo al PATH global ni
modificar `~/.codex/config.toml`. No usar instalaciones `latest`, `npx` remoto ni
actualizaciones automáticas como recuperación de un fallo.

Para preparar otra Mac Apple Silicon, descargar el asset y `sha256sum.txt` del
[release v1.14.3](https://github.com/alibaba/open-code-review/releases/tag/v1.14.3)
en un directorio externo de tarea. Comprobar su SHA256 contra tool.json y el archivo
de checksums **después** de terminar la descarga, antes de hacerlo ejecutable.
Instalar en la ubicación anterior y conservar recibo de versión/hash y licencia del
proveedor. Para otra plataforma, verificar su asset y checksum y revisar el pin
antes de usarlo; no ejecutar el binario ARM64 ni cambiar el pin silenciosamente.

```bash
OCR_BIN="$HOME/.local/share/open-code-review/v1.14.3/ocr"
shasum -a 256 "$OCR_BIN"
"$OCR_BIN" --version
```

Si falta o no coincide, registrar el bloqueo de OCR y continuar sólo el trabajo
que siga autorizado. Su instalación no autoriza inferencia nativa pagada. Un alias
de modelo configurado que la cuenta rechace tampoco demuestra que otro modelo
sea equivalente: registrar el modelo real, sin cambiar configuración global.

## Evidencia por revisión

Guardar fuera del repositorio, bajo la tarea, preview, reglas, refs/huellas,
hallazgos/adjudicación y registro de lecturas del anfitrión. Para rangos, fijar base
y destino a SHAs y conservar el merge-base devuelto; las reglas de estos dos
comandos deben resolver el mismo destino. Usar `--rule` explícito del proyecto.
Si el destino cambia, la evidencia anterior no valida el nuevo SHA.

Inventario mínimo de cobertura:

| Campo | Contenido |
|---|---|
| archivo/estado | Identidad del archivo en Git, incluidas bajas y recreaciones |
| selección | Incluido/excluido y motivo real de preview |
| lectura | reviewed, partial, context_only o skipped |
| evidencia | Rangos y referencia de llamadas/eventos realmente examinados |
| límite | Truncamiento, falta de contexto u otra razón pendiente |

Comparar el inventario con Git completo, preview y las llamadas del revisor. Los
archivos sólo consultados como contexto no cuentan como revisión completa. Las
lecturas parciales de excluidos quedan visibles y requieren corregir la selección
si se decide revisarlos; una tabla del agente no prueba cumplimiento por sí sola.
Si no se dispone de traza verificable, decirlo y no declarar cobertura verificada.

Informar denominadores separados: archivos cambiados y archivos seleccionados.
El 100% de seleccionados puede ocultar omisiones del cambio. No contar dos veces
contexto y revisión ni afirmar que todo fue leído porque el comando terminó bien.

Los hallazgos requieren escenario alcanzable y archivo/consumidor que lo sostenga.
Separar estático de reproducido y pendiente de descartado; conservar el motivo de
adjudicación. La falta de token/versionado, una mejora preventiva o una corrección
histórica no bastan por sí solas para demostrar corrupción o vulnerabilidad.

El tiempo y consumo pertenecen a la revisión real. Si hay JSONL del CLI, conservar
`turn.completed.usage`: caché es parte de entrada y razonamiento parte de salida.
Sin datos del anfitrión, informar que consumo no está disponible, sin estimarlo ni
convertir suscripción a MXN. Separar revisión, preparación y adjudicación.

## Criterio de adopción

El piloto histórico del 24-nov-2026 justificó una segunda lectura selectiva, no
una garantía ni una mejora general de velocidad: los métodos detectaron problemas
distintos, ambos omitieron defectos conocidos y la selección por defecto dejó
pruebas/documentos fuera. Esta configuración corrige la selección para los tipos
indicados; la inspección real del anfitrión sigue requiriendo evidencia.

No se añade un gate automático nuevo a CI ni se sustituyen las pruebas o la
revisión habitual. Medir posteriores usos sobre SHAs concretos antes de ampliar
obligatoriedad, concurrencia o proveedores. La regla local se valida en su archivo
y en la selección real de OCR, sin reiniciar ni desplegar la aplicación.

Referencia del proveedor: [workflow de delegación fijado al commit del release](https://github.com/alibaba/open-code-review/blob/f6379f71edba1e59dbc056e8f455df1f84396498/skills/open-code-review-delegate/SKILL.md).
La skill local está redactada para este proyecto y usa esa interfaz, sin instalar
el flujo nativo ni sus instrucciones de bootstrap automático.

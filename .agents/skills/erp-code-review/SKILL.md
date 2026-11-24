---
name: erp-code-review
description: Use when a Pollyana ERP change needs a second code review because it materially affects shared data, concurrency, queues, or integration contracts, or when Mauricio explicitly requests OCR review. Not for every PR or a full security audit.
---

# Segunda revisión con OCR delegado

Leer [el procedimiento del proyecto](../../../docs/agents/code-review.md).
OCR selecciona archivos y resuelve reglas; Codex hace el análisis con su modelo
y autenticación actuales. Esta segunda lectura complementa la revisión habitual.

## Preparar una revisión reproducible

- Delimitar riesgo, base y destino; fijar SHAs. Leer AGENTS de módulos y consumidores.
- Usar el CLI local verificado de `.opencodereview/tool.json`. No instalar/actualizar
  automáticamente, configurar otro proveedor ni sustituirlo por `ocr review`.
  Si falta, informar ese prerrequisito y seguir la revisión habitual permitida.
- Mantener la fuente en sólo lectura. Las salidas van al directorio externo de
  evidencia de la tarea. No ejecutar código ERP, pruebas o servicios sólo por
  invocar esta skill; las validaciones de implementación tienen su flujo propio.
- Usar el mismo modo, refs y `--rule` en preview y rule. Para una rama:

```bash
OCR_BIN="$HOME/.local/share/open-code-review/v1.14.3/ocr"
"$OCR_BIN" delegate preview --repo "$REPO" --from "$BASE_SHA" --to "$HEAD_SHA" --rule "$REPO/.opencodereview/rule.json" --format json
"$OCR_BIN" delegate rule --repo "$REPO" --from "$BASE_SHA" --to "$HEAD_SHA" --rule "$REPO/.opencodereview/rule.json" --format json -- "${FILES[@]}"
```

`REPO`, refs y `FILES` son valores de la tarea, nunca texto de un diff interpolado
como código shell. Para un commit usar `--commit SHA` en ambos comandos; para
cambios sin confirmar omitir refs y registrar la huella del diff y archivos nuevos.

## Revisar sin confundir selección y cobertura

1. Cotejar preview con el inventario Git completo. Revisar todas las exclusiones;
   pruebas y documentación deben entrar con la configuración del proyecto.
   Si un contrato relevante no entra, corregir la selección explícitamente y
   repetir preview/rule, sin levantar filtros de secretos o binarios.
2. Leer el diff completo y las reglas de cada archivo seleccionado, en lotes si
   hace falta. Usar el grafo para consumidores cuando represente el SHA revisado;
   si no, documentar el fallback local. Para borrados, inspeccionar el lado anterior.
3. Llevar una fila por `(path, status)` de preview y una lista de contexto adicional.
   No leer un archivo excluido a escondidas: registrar el motivo y la exposición
   parcial, sin convertirla en revisión completa ni ocultar la desviación.
4. Por archivo registrar `reviewed`, `partial`, `context_only` o `skipped`, con
   razón, rangos realmente examinados y referencias a llamadas/herramientas o
   eventos de lectura. Un comando emitido o una salida truncada no demuestra que
   se examinó todo el contenido. Contrastarlo antes de declarar cobertura.
5. Seguir un escenario concreto hasta sus consumidores y contratos. Contrastar
   tablas SQL, fechas/NULL, colisiones, contexto durable y controles de UI cuando
   corresponda. No detenerse al encontrar el primer problema.

## Entregar y comprobar

- Hallazgos con archivo/línea, escenario, impacto, evidencia y certeza. Separar
  confirmados estáticos, pendientes y descartados con motivo; no borrar la pista
  de adjudicación. No convertir hardening en vulnerabilidad sin ruta alcanzable.
- Informar total de archivos cambiados, seleccionados, excluidos, revisados
  completamente, parciales y sólo contexto. Dar cobertura sobre el total del
  cambio y sobre seleccionados; contar cada archivo una sola vez por denominador.
  Mostrar las desviaciones y la evidencia ausente. Sin traza, cobertura declarada
  no verificada; no usar 100% como garantía de ausencia de defectos.
- Registrar SHAs/huella, versión/hash de OCR, modelo real disponible, duración y
  tokens si el anfitrión los expone. Separar entrada, caché y salida; no inventar
  consumo, dinero ni equivalencia con otro modelo.
- Si cambia código, configuración, base o destino, invalidar la revisión afectada.
  Mantener gates de CI y autorizaciones de merge/despliegue del proyecto.

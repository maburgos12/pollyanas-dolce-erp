# Salida compacta del grafo para Codex

`scripts/codebase_memory_compact.py` es un adaptador stdio para el servidor
existente `codebase-memory-mcp`; no sustituye su binario ni modifica los índices.
Se instala sólo en la configuración local de Codex. No afecta al ERP desplegado.

## Información conservada

En `search_graph` y `get_code_snippet`, la salida predeterminada omite únicamente
`fp`, `sp` y `bt` de objetos de símbolos identificados por `qualified_name` y
`file_path`. Son representaciones internas para comparar código. Se conservan
íntegros código, documentación, firmas, rutas, líneas, complejidad, scores,
vecinos, paginación, totales, advertencias y campos desconocidos. No se limita el
número de resultados ni se recorta el código.

La respuesta indica los campos omitidos en `_response_detail`. Para obtener la
respuesta original, pasar `response_detail="full"` en esas dos herramientas.
El adaptador consume ese argumento antes de invocar al servidor nativo. Los
errores, recursos, notificaciones y las demás herramientas pasan sin filtrar.
En particular `query_graph` conserva incluso consultas explícitas a `fp/sp/bt`.
No usar la ausencia de una arista como prueba de ausencia de dependencia.

## Instalación y reversión local

Requiere Python 3.11+ para el instalador; el runtime usa sólo la biblioteca
estándar y admite Python 3.9+. Vista previa sin cambios:

```sh
python3 scripts/install_codebase_memory_compact.py
```

Aplicar con `--apply` dentro del alcance autorizado. Sólo se cambian `command` y
`args` de `[mcp_servers.codebase-memory-mcp]`; se conservan el ejecutable nativo,
sus argumentos, variables, límites y permisos de herramientas. El runtime se
copia a `~/.codex/tools/codebase-memory-compact/<sha>/`, independiente del
worktree, y se respalda `config.toml` antes de reemplazarlo atómicamente. No se
imprime el contenido de la configuración. El recibo incluye hashes y respaldo.

La configuración se usa al abrir una conexión MCP nueva. Las conexiones ya
abiertas conservan el proceso anterior; recargar/reiniciar Codex permite usarla
en ellas. Verificar con una conexión real creada desde la configuración
instalada, no sólo con la lectura del TOML.

Para revertir, restituir `command` y `args` del servidor desde el respaldo que
nombra el recibo. Restaurar el archivo completo sólo si su hash actual coincide
con `config_after_sha256` del recibo; de otro modo preservar los cambios de otras
tareas y restaurar únicamente esos dos campos. No borrar índices ni el binario
original. Las actualizaciones del instalador nativo pueden reescribir su entrada;
revisar de nuevo con el instalador local, sin reinstalar por rutina.

## Verificación y ahorro

```sh
python3 -m unittest discover -s scripts/tests -p test_codebase_memory_compact.py
```

Las pruebas verifican conservación de información, escape a salida completa,
correlación de respuestas, notificaciones, stderr, salida del proceso hijo,
configuración reversible, idempotencia y rechazo de cambios concurrentes.
CI ejecuta estas pruebas dentro de su gate habitual.

El 24/nov/2026, en seis consultas dirigidas del ERP y 18 llamadas reales, el
runtime instalado redujo respuestas de 12,060 a 9,884 tokens normalizados
(`o200k_base`): 18.0%. Se comprobó igualdad de todos los campos conservados y del
código completo frente a `response_detail="full"`. No es una medición del gasto
total de Codex. Es menor que la proyección experimental de 29%, porque esta
implementación conserva también métricas y otros metadatos útiles.

## Dependencias de costeo: diagnóstico separado

En `codebase-memory-mcp 0.8.1`, el grafo devuelve cuatro de nueve consumidores
explícitos de `rentabilidad.services_costos.asignar_costos_filas`. Los cinco
faltantes usan imports relativos en cuatro archivos. No es un problema del
filtro compacto: `trace_path` pasa sin cambios y el índice nuevo reproduce el
fallo.

Un ejemplo pequeño detecta imports relativos sin alias. Al añadir 150 archivos
inocuos, se activa la ruta paralela y desaparecen esas llamadas, mientras la
llamada con import absoluto permanece. Un alias relativo mostró además otra
omisión, también en el caso pequeño; no se considera resuelta por este ensayo.

En una copia estática aislada del ERP al SHA
`b6c3e354f34c060b946710c28444ece2574ed6a1`, normalizar sólo los imports
`.services_costos` a `rentabilidad.services_costos` en `services_resultado.py`,
`views_rentabilidad.py`, `tests_costos_fuente.py` y
`tests_produccion_documental.py` recuperó los nueve consumidores. El código y los
datos operativos del ERP se conservaron.

La corrección preferida pertenece al indexador: canonicalizar imports relativos
Python antes de enlazar tipos y llamadas, y verificar paridad de la resolución
secuencial y paralela. No convertir el import en una llamada ni añadir aristas a
mano. Revisar los límites de la resolución LSP y el fallback sólo hacia símbolos
demostrados por el import, evitando enlaces por coincidencia de nombre.

Fuentes de la versión examinada:
[resolución paralela](https://github.com/DeusData/codebase-memory-mcp/blob/v0.8.1/src/pipeline/pass_parallel.c),
[resolución secuencial](https://github.com/DeusData/codebase-memory-mcp/blob/v0.8.1/src/pipeline/pass_calls.c),
[enlace de imports Python](https://github.com/DeusData/codebase-memory-mcp/blob/v0.8.1/internal/cbm/lsp/py_lsp.c).
La ruta paralela puede descartar una llamada cuando hay resolución LSP pero su
destino no coincide con un nodo; la secuencial intenta después el registro.
La evidencia local identifica el comportamiento y una normalización que lo
corrige; no constituye una corrección compilada del binario del proveedor.

Antes de adoptar un parche/versión, probar imports absolutos, relativos, alias,
imports de paquetes externos, nombres repetidos y sombreado de variables en
ambas rutas, usando una fixture suficientemente grande para activar la paralela.
Después reconstruir un índice aislado y cotejar los nueve consumidores con la
fuente. No cambiar globalmente a modo secuencial ni actualizar todos los índices
sin comprobar la cobertura de los demás lenguajes.

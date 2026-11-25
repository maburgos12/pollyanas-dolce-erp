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

## Dependencias de costeo: diagnóstico original (24/nov/2026)

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

## Corrección nativa local (25/nov/2026)

El parche `scripts/patches/codebase-memory-mcp-0.8.1-python-relative.patch`
se aplica exclusivamente al commit upstream
`f0c9be19c5d74b84f418d807bfdce7b5d6a261ff` (v0.8.1).
Su procedencia y SHA256 están en el JSON contiguo. Es una compilación local,
no un release publicado por el proveedor. No cambia los imports del ERP.

- Canonicaliza los imports relativos antes de emitir resoluciones LSP.
- Evita añadir un punto adicional en `from . import nombre`, incluyendo alias.
- La ruta secuencial conserva la protección de la paralela: un destino LSP
  externo o sin indexar no cae en una función local sólo por llamarse igual.
- Añade regresiones C para import hermano, padre, `__init__` y límite del proyecto.
  No modifica las herramientas MCP ni el filtro compacto. El soporte general
  de alias de funciones Python sigue siendo una limitación separada.

Para reproducir, usar una copia limpia del proveedor en ese commit, verificar
el hash del parche contra el manifiesto y ejecutar desde esa copia:

```sh
git apply --check /RUTA_ERP/scripts/patches/codebase-memory-mcp-0.8.1-python-relative.patch
git apply /RUTA_ERP/scripts/patches/codebase-memory-mcp-0.8.1-python-relative.patch
bash scripts/test.sh
bash scripts/build.sh --version 0.8.1-pollyana-relative1
```

La compilación usa las dependencias incluidas por el proveedor. Conservar su
licencia MIT. No ejecutar sus comandos de instalación/actualización global.
Probar el binario resultante desde el worktree del ERP con:

```sh
python3 scripts/verify_codebase_memory_indexer.py --binary /RUTA_PROVEEDOR/build/c/codebase-memory-mcp
```

La regresión crea fixtures estáticas pequeñas y con 150 archivos adicionales,
verifica exactamente nueve consumidores en ambas rutas y rechaza enlaces al
homónimo de otro paquete y al import externo. Usa caché/configuración aisladas,
no ejecuta Python del ERP y retira sus archivos e índices incluso ante fallos.

Tras verificar, copiar el binario a una ubicación permanente e inmutable por
SHA256 fuera del worktree; conservar el original. Para seleccionar esa copia
sin retirar la salida compacta, revisar la vista previa y aplicar:

```sh
python3 scripts/install_codebase_memory_compact.py --upstream-command /RUTA_PERMANENTE/codebase-memory-mcp
python3 scripts/install_codebase_memory_compact.py --upstream-command /RUTA_PERMANENTE/codebase-memory-mcp --apply
```

El instalador exige un archivo ejecutable y conserva argumentos anteriores,
permisos y el resto del TOML. Genera un respaldo nuevo; la reversión descrita
arriba también sirve para volver al binario original. Abrir una conexión nueva
antes de comprobarlo: cambiar la configuración no sustituye procesos abiertos.
Reconstruir el índice afectado con el binario corregido; los índices anteriores
no recuperan las aristas faltantes por cambiar solamente el ejecutable.

Se repitieron las 18 llamadas sobre la misma copia estática del ERP
`b6c3e354f34c060b946710c28444ece2574ed6a1`, sin normalizar imports:

| Indexador | Respuesta completa | Compacta | Ahorro | Cobertura de llamadas |
|---|---:|---:|---:|---:|
| 0.8.1 original | 12,060 | 9,884 | 2,176 (18.04%) | 37/42 |
| 0.8.1 con parche | 12,245 | 10,069 | 2,176 (17.77%) | 42/42 |

Costeo pasa de 4/9 a 9/9. Se cotejaron identidades, no sólo cantidades. Los 185
tokens adicionales contienen las dependencias recuperadas; el ahorro absoluto
permanece intacto. Se verificó igualdad de código y de todos los campos retenidos
entre salida completa y compacta; `trace_path` sigue sin filtrado. Estas cifras
miden contexto recuperado con `o200k_base`, no el gasto total de una sesión.

# Pollyana’s Dolce ERP — instrucciones de agentes

ERP interno de nueve sucursales en Sinaloa. Idioma operativo: español;
zona horaria `America/Mazatlan`, moneda MXN. Mauricio Burgos es Director General.
Estas reglas complementan las instrucciones globales; no amplían autorizaciones.

## Alcance y autorización

- Consulta, diagnóstico, revisión o propuesta: sólo lectura hasta autorización
  de implementación. Traducir la necesidad a aceptación observable y revisar
  fuentes, consumidores y contratos antes de decidir. Elegir el cambio mínimo completo.
- Ejecutar autónomamente los pasos ya autorizados. Pedir aprobación previa ante
  arquitecturas con consecuencias distintas, alcance ambiguo/ampliado, seguridad,
  permisos, autenticación, producción, datos operativos o contratos compartidos
  (API, modelos, migraciones, integraciones, eventos, caché/service worker).
- No alterar nómina, RRHH, ventas, inventario, datos maestros o configuración
  operativa sin petición explícita. Nunca borrar migraciones ni modificar las
  aplicadas. No reset/drop, `.env` productivo, puertos Compose, eliminación de
  modelos/urls ni force-push a main sin autorización explícita.
- Un `AttributeError` no prueba una migración faltante. Prohibido `migrate --fake`
  y `--fake-initial` sin autorización explícita de Mauricio para la migración concreta;
  cotejar código cargado, historial y esquema real según el procedimiento enlazado.
- Codex responde por diagnóstico, ejecución, revisión, validación y entrega;
  no esperar a Claude ni exigir su plugin. No declarar pruebas, entregas o avisos
  futuros que no estén respaldados por evidencia y supervisión real.

## Inicio desde la raíz: índice obligatorio por alcance

No asumir que Codex cargará automáticamente los AGENTS descendientes cuando el
chat comienza en la raíz. **Antes de diseñar o editar**, identificar las rutas
previstas y leer explícitamente cada AGENTS aplicable de esta tabla, desde la raíz
del worktree. Leer también los AGENTS de todos los consumidores afectados y
seguir sus referencias compartidas. Si cambia el alcance, repetir la selección;
si falta un destino, detener la edición de ese alcance e informar el enlace roto.
Para módulos no listados, aplicar este archivo y las guías transversales siguientes.

| Rutas o alcance afectado | Leer antes de actuar |
|---|---|
| `bonos_produccion/**` | [Bonos producción](bonos_produccion/AGENTS.md) |
| `bonos_ventas/**` | [Bonos ventas](bonos_ventas/AGENTS.md) |
| `reportes/**`, mapeo de rubros, presupuesto vs real | [Reportes](reportes/AGENTS.md) |
| `rentabilidad/**`, márgenes y resultados | [Rentabilidad](rentabilidad/AGENTS.md) |
| `orquestacion/**`, resúmenes/clasificación/extracción aptos para IA local | [Orquestación](orquestacion/AGENTS.md) |
| `scripts/**`, CI, herramientas de operación | [Scripts](scripts/AGENTS.md) |

| Disparador transversal | Guía obligatoria |
|---|---|
| Toda modificación, commit, PR o cierre | [Ciclo de trabajo](docs/agents/workflow.md) |
| Segunda revisión de cambios de datos, concurrencia o integraciones | [OCR delegado](docs/agents/code-review.md) y [skill erp-code-review](.agents/skills/erp-code-review/SKILL.md) |
| Despliegue, migraciones o diagnóstico productivo | [Producción](docs/agents/deployment.md) |
| UI, formularios, navegación, autenticación, PWA, móvil, diseño, branding, tienda online o marketing | [Validación UI](docs/agents/ui.md), [PRODUCT](PRODUCT.md) y [DESIGN_STACK](DESIGN_STACK.md) |
| Capturar, importar o calcular datos compartidos | [Skill erp-data-reuse](.agents/skills/erp-data-reuse/SKILL.md) y [ficha de fuentes](docs/data-reuse/ficha-fuentes-template.md) |
| Tarea inscrita explícitamente en el piloto de fábrica | [Software factory](docs/software-factory.md) |

Las guías son instrucciones obligatorias bajo su disparador, no lecturas de todas
las tareas. El índice sólo enruta; no sustituye leer el destino ni comprobar fuentes.
Si hay varios módulos, cumplir la unión de reglas y resolver contradicciones antes
de editar; ninguna guía permite ampliar el alcance autorizado.

## Entorno y entrega

- Implementar exclusivamente en rama `codex/<modulo>-<descripcion>` y worktree
  propios registrados por `scripts/task_workspace_start.sh`; ejecutar preflight
  antes de escribir. El checkout base `pastelerias_erp_sprint1` es plano de control
  de sólo lectura. No crear worktrees con comandos manuales ni cambiar ramas ajenas.
- PostgreSQL 16 es la única base soportada. Antes de cualquier `manage.py`,
  comprobar configuración PostgreSQL y conectividad; aislar y registrar proyecto
  Compose, puerto y volumen. No usar SQLite ni compartir bases mutables entre tareas.
- Sólo documentación/instrucciones/configuración Codex: validar contenido, rutas,
  symlinks, TOML y diff; sin bases ni checks Django. Código/configuración de aplicación:
  `check` antes de commit, migraciones y pruebas afectadas sobre PostgreSQL;
  UI en navegador real. Conservar CI remoto requerido en ambos casos, sin bypass.
- Para aplicación: PR revisado, CI `test` exitoso del SHA vigente, merge autorizado,
  deploy oficial **sin pull manual previo**, verificación del consumidor real y
  cierre registrado. No sustituir el resultado por un PR mergeado o prueba local.
  Documentación/configuración Codex no requiere despliegue ni reinicio de aplicación.
- No retirar ramas/worktrees manualmente: usar cierre/handoff registrado con dueño,
  motivo y fecha de revisión. No ejecutar `git branch -D` fuera del flujo respaldado.
  Cumplir el cierre global de Docker local; conservar recursos ajenos/activos y
  producción. No declarar cierre con recursos propios pendientes sin entrega.
- `CLAUDE.md` es un symlink relativo a este archivo: mantener una sola fuente,
  no sustituirlo por una copia. `cmp AGENTS.md CLAUDE.md` debe seguir pasando.

## Fuentes, evidencia y comunicación

- Preferir el grafo MCP para descubrir código; usar búsquedas acotadas de literales,
  configuración/documentación o fallback si el grafo es insuficiente. Verificar
  código vigente antes de usar mapas o versiones recordadas; no reindexar por rutina.
- Antes de duplicar capturas/maestros, aplicar `erp-data-reuse`, inventario de fuentes
  y consultas acotadas con PostgreSQL. Sus candidatos léxicos no prueban identidad;
  presentar equivalencias nuevas para revisión antes de aplicarlas.
- Ninguna cifra sin fuente verificable: comprobar sumas contra la fuente, separar
  detalle/agregados, presupuesto/real e ingresos/egresos. Identificar inferencias
  y estimaciones como tales; no inventar datos ni validar contra constantes propias.
  Si falta evidencia, decir qué falta. Invalidar y corregir cifras erróneas.
- Entre personal, usar primero correo institucional de Pollyana’s Dolce; verificar
  remitente y destinatarios antes de borrador/envío. Sin cuenta institucional,
  informar bloqueo; otra cuenta requiere instrucción expresa. Borrador no autoriza envío.
- Seleccionar y leer la skill más específica antes de aplicarla, avisar brevemente;
  no activar todas ni instalar por defecto. Para UI seguir el enrutamiento de diseño.
- Avances breves sobre decisiones/bloqueos; entrega con resultado, evidencia,
  limitaciones y recursos pendientes. Continuar el trabajo autorizado hasta cumplir
  aceptación; si falta una decisión reservada o acceso real, formular una petición concreta.

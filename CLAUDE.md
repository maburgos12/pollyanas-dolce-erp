# CLAUDE.md — Pollyana's Dolce ERP

## Identidad del proyecto
Sistema ERP operativo para Pollyana's Dolce, cadena de pastelerías con 9 sucursales en Sinaloa, México.
Director General: Mauricio Burgos. Sistema de uso interno para el equipo operativo.

## Servidor de producción
- Host: 68.183.165.47 · Usuario: root · SSH: ~/.ssh/agente_dg_ops
- Directorio: /opt/pastelerias-erp
- Comando base: `docker compose -f /opt/pastelerias-erp/docker-compose.yml`
- URL: https://erp.pollyanasdolce.com
- Rama principal: main · Repo: github.com/maburgos12/pollyanas-dolce-erp (privado)

## Stack
- Backend: Django 5.0.1 + DRF 3.14 · DB: PostgreSQL 16 (prod) / SQLite (dev)
- Deploy: Docker + VPS propio (NO Railway)
- Servidor: Gunicorn + WhiteNoise · Zona horaria: America/Mazatlan · Moneda: MXN

## Contexto de producto y diseño
Para cualquier tarea de UI, frontend, diseño visual, experiencia de usuario,
branding, tienda online o marketing, leer primero:
- `PRODUCT.md`: estrategia del producto, usuarios, principios, anti-referencias
  y accesibilidad.
- `DESIGN_STACK.md`: enrutamiento de skills entre `impeccable`, `hallmark`,
  `emil-design-eng` y los skills de Leon.

No duplicar las reglas completas de diseño en este archivo. `CLAUDE.md` conserva
el protocolo operativo del ERP; `DESIGN_STACK.md` es la fuente compartida para
ejecución de diseño.

## Deploy manual (NO es automático al mergear)
```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```
Si migrate falla por columna duplicada: `migrate <app> <numero> --fake` en la migración específica.
`restart web` queda reservado para cambios de imagen, dependencias del contenedor o variables de entorno; para código y estáticos normales se usa recarga `HUP` de Gunicorn para no abrir una ventana de `502`.

## Backups
- Automático: diario 2am via cron
- Script: /opt/pastelerias-erp/scripts/backup_db.sh · Destino: /opt/backups/erp/ · Retención: 7 días

## Otros sistemas en el mismo servidor
| Sistema | Directorio | Puerto | Dominio |
|---------|-----------|--------|---------|
| Maya omnicanal | /opt/pollyana-omnichannel | 8003 | api.pollyanasdolce.com |
| ERP Django | /opt/pastelerias-erp | 8011 | erp.pollyanasdolce.com |

---

## División de trabajo Claude / Codex (regla permanente)

Combinación deliberada por costo: Codex (plan alto) absorbe los tokens caros;
Claude (plan limitado) se reserva para el criterio. Codex está integrado vía el
plugin `codex@openai-codex` (comandos `/codex:rescue`, `/codex:review`,
`/codex:adversarial-review`). Requisito: Codex CLI instalado y autenticado
(`/codex:setup` debe reportar `ready: true`).

### Claude = cerebro / orquestador
- Diseño, decisiones de arquitectura y lógica de negocio (costeo, márgenes,
  nómina, bonos, MRP — todo lo delicado o irreversible).
- Preparar rama limpia desde `origin/main`, definir el contrato/approach.
- Validación final: `check`, `migrate --check`, tests, navegador, deploy en VPS.
- Commit / PR / deploy SIEMPRE los hace Claude siguiendo el protocolo de abajo.

### Codex = talacha pesada (lo que más consume tokens)
Delegar a Codex cuando el trabajo sea: implementación mecánica repetida, refactor
masivo en muchos archivos, baterías de tests, migración de patrones, normalización
de datos, o debugging largo. Codex trabaja sobre el working tree real → SIEMPRE en
rama aislada, nunca directo sobre algo que pueda tocar producción sin revisar.

**Cómo delega Claude (mecanismo real — esto es lo que de verdad dispara Codex):**
- Claude NO "teclea" `/codex:rescue`. Esos slash-commands los escribe Mauricio.
- Cuando Claude decide delegar por su cuenta, invoca la herramienta **Agent** con
  `subagent_type: "codex:codex-rescue"` y le pasa la tarea como prompt. Ese
  subagente reenvía el trabajo al runtime de Codex (write-capable por defecto).
- Si Claude solo describe el reparto pero hace el trabajo él mismo, NO está
  cumpliendo esta regla. El acto de delegar = una llamada a Agent(codex:codex-rescue).

**Cómo lo dispara Mauricio (garantizado, sin depender del criterio de Claude):**
- `/codex:rescue <tarea>` → Codex implementa. `/codex:review` → Codex revisa (read-only).

**Trampas que impiden que la regla aplique (verificar si "no funciona"):**
- El plugin `codex@openai-codex` debe estar instalado y cargado en ESA sesión
  (`/codex:setup` → `ready: true`; si se acaba de instalar, `/reload-plugins`).
- Claude debe haberse abierto desde `pastelerias_erp_sprint1/` (este CLAUDE.md).
  Desde otra carpeta carga otro CLAUDE.md que no tiene esta regla.
- Esta regla debe estar mergeada en `main`; si solo vive en una rama, otras
  sesiones no la ven.

**Reglas de aislamiento y aviso (aprendidas en producción — NO ignorar):**
- **Nunca cambiar de rama en el working tree donde Codex está corriendo.** Codex
  lee/edita esos archivos en vivo; un `git checkout` le quita el piso y el job
  muere huérfano (estado `running` falso, reporte perdido). Delegar SIEMPRE en un
  `git worktree` dedicado y no tocar esa carpeta hasta que Codex termine. Ojo:
  este repo tiene muchas sesiones/worktrees en paralelo que pueden mover la rama
  del working tree principal solas.
- **Los jobs en `--background` NO avisan al terminar.** Se consultan a mano con
  `/codex:status` y se traen con `/codex:result` (o `codex-companion.mjs
  status|result`). Para tener aviso automático + reporte completo, correr Codex
  en `--wait` dentro de un background task del harness (ese sí notifica al salir),
  o montar un poller. Si un job quedó huérfano, limpiarlo con `/codex:cancel`.

### Lo que NO se delega a Codex
- Lógica que pisa datos de nómina/RRHH/ventas (ver "Datos de usuarios — NUNCA pisar").
- Migraciones, `.env`, puertos, `settings.py`, push a `main`.
- La decisión de commitear/mergear/deployar: ese filtro final es de Claude.

### Criterio de enrutamiento
Tarea acotada y rápida → la hace Claude. Tarea voluminosa, repetitiva o de
iteración larga → se delega a Codex. Ante la duda sobre algo delicado, lo
diseña Claude y Codex solo ejecuta la parte mecánica.

---

## Protocolo obligatorio — lo sigo siempre

Aplica para cualquier solicitud de Mauricio que implique tocar código, datos,
configuración, UI, permisos, navegación, reportes, jobs, integraciones,
prototipos o producción.

### 1. Clasificar el tipo de trabajo antes de tocar archivos
- **Consulta o diagnóstico:** solo leer, inspeccionar y reportar. No modificar nada
  salvo que Mauricio lo pida explícitamente.
- **Prototipo aislado:** mantenerlo separado del ERP real. No usar producción para validar ideas.
- **Prototipo dentro del ERP local:** usar rama limpia y Docker local solo como entorno
  de prueba. No presentarlo como validación de producción.
- **Implementación real:** trabajar en rama limpia, con checks, PR, deploy y validación
  en el lugar real donde se usa.

### 2. Sincronizar entorno local con main ANTES de tocar cualquier archivo

**Obligatorio al inicio de cada tarea, sin excepción.**
El SQLite local puede estar días o semanas detrás de `origin/main`. Si no se sincroniza
primero, aparecen migraciones "pendientes" de otras apps que no son de la tarea actual,
`manage.py check` falla con ruido ajeno y el entorno no es confiable.

```bash
git fetch origin main
git checkout -b codex/<modulo>-<descripcion> origin/main   # rama limpia desde main actualizado
python manage.py migrate                                    # aplicar TODO lo que main ya tiene
python manage.py migrate --check                           # debe quedar en 0 pendientes
python manage.py check                                     # debe quedar en 0 errores
```

**Regla:** si `migrate --check` no es 0 antes de empezar a escribir código, detener
y aplicar las migraciones pendientes primero. Nunca iniciar una tarea sobre un entorno
de DB desactualizado.

### 3. Auditar estado del repo antes de cualquier cambio
```bash
git branch --show-current        # confirmar rama correcta
git status --short --branch      # sin cambios ajenos sin commitear
git diff --stat                  # qué hay modificado
git branch -vv                   # relación con origin
git rev-list --left-right --count origin/main...HEAD  # cuánto detrás/adelante
```
**Detenerse y reportar si:**
- hay cambios sin commitear que no pertenecen a la tarea
- hay mezcla de módulos no relacionados en el historial
- la rama no tiene upstream y se pretende subir
- la rama está muy detrás de `origin/main` sin plan explícito
- el objetivo no corresponde al nombre o historial de la rama

En esos casos: respaldar parche, crear rama limpia desde `origin/main` y aplicar
solo los cambios relacionados.

### 4. Una tarea, una rama, un objetivo
- No mezclar RRHH + bonos + reportes + CSS + activos en una sola rama
- No hacer refactors oportunistas ni tocar archivos que no son de la tarea
- Nombre de rama: `codex/<modulo>-<descripcion>` o `fix/<descripcion>`
- Para cambios grandes o productivos, crear rama nueva desde base limpia
- Nunca dejar pares fix+revert sin squash — generan ruido y confusión

### 5. Docker local no equivale a producción
- Si Docker local falla, diagnosticar logs y variables de entorno primero;
  no cambiar código ni producción por intuición
- No modificar `.env` de producción ni puertos de `docker-compose.yml` sin
  confirmación explícita de Mauricio
- Una validación local no sustituye la validación final en VPS, navegador real,
  reporte real, pantalla real o usuario real afectado

### 6. Higiene de commits — quirúrgico y descriptivo
Antes de cualquier commit:
```bash
git status --short --branch
git log --oneline --decorate -5
git worktree list
```
- Confirmar **solo archivos relacionados con la tarea**
- Si hay cambios ajenos: `git stash push -u -m "resguardo-<tarea>-<fecha>"` y reportar
- Nunca commitear: `.DS_Store`, `.playwright-mcp/`, `.claude/`, `outputs/`,
  `*.png/jpg/gif` fuera de `static/`, `storage/dg_reports/`, logs temporales
- Mensaje descriptivo: qué cambió y por qué, no solo "fix"

### 7. Pull requests — una tarea, un PR
Antes de crear o aprobar cualquier PR:
```bash
git diff origin/main..HEAD --stat   # qué archivos cambian
git log --oneline --decorate -5     # historial limpio sin mezcla
```
- Verificar que la rama no mezcle tareas distintas
- No abrir PR con `python manage.py check` con errores
- No aprobar PR de Codex sin revisar el diff aquí primero

### 8. Validación mínima antes de cerrar
- `python manage.py check` → 0 errores antes de cualquier commit
- `python manage.py migrate --check` → sin migraciones pendientes antes de deploy
- Tests del módulo afectado cuando existan
- Para UI, permisos, PWA, formularios o flujos visibles: validar en navegador real
  con consola y Network/XHR
- Para datos operativos: validar conteo/registros en tabla y confirmar que aparecen
  en la pantalla, reporte o app donde se usan

### 9. Cierre responsable
No declarar terminado si solo compila, solo responde la API, o solo la base tiene datos.
Termina cuando el resultado está validado en el flujo real. Si no se puede validar,
reportar el bloqueo exacto, lo que sí quedó hecho y qué falta para confirmar.

---

## Lecciones críticas aprendidas en producción (NO ignorar)

### Migraciones — regla de oro
- Antes de cualquier PR que toque modelos:
  ```bash
  python manage.py migrate --check
  python manage.py showmigrations bonos_produccion bonos_ventas rrhh
  ```
- Si producción rompe con `AttributeError: object has no attribute 'campo'`
  → la migración no se aplicó → `migrate <app> <num> --fake` + restart. No tocar código.
- Si campos ya existen en BD pero la migración no está registrada: usar `--fake`
- NUNCA borrar migraciones existentes
- NUNCA modificar migraciones ya aplicadas en producción

### PR abierto = tarea sin terminar — deploy obligatorio antes de continuar
Un PR mergeado sin deploy en VPS equivale a un cambio que no existe para el usuario.
No pasar a una segunda tarea hasta que la primera esté deployada y validada en producción.

**Prohibido:**
- Crear PR y abrir otra tarea sin deployar primero
- Copiar archivos al VPS por SCP como atajo al flujo git — `git pull` es el único canal válido
- Declarar "listo" cuando el VPS aún corre el código anterior

**Flujo mínimo obligatorio al cerrar cualquier cambio de código:**
1. PR mergeado a main
2. `git pull origin main` en VPS
3. `docker compose restart web` en VPS
4. Verificar resultado visible en producción
5. Borrar la rama de trabajo local y remota cuando ya esté mergeada, deployada
   y validada, para que no se atraviese ni aparezca como opción en otros hilos:
   `git branch -D <rama>` y `git push origin --delete <rama>`; después correr
   `git fetch --prune origin`.

### Datos de usuarios — NUNCA pisar
`bono_extra`, `ajuste_positivo`, `ajuste_negativo` son datos de nómina real capturados
por las jefas. Un borrado accidental es crítico.
- **NUNCA** resetearlos, sobreescribirlos ni incluirlos en seeds o inicializaciones
- **NUNCA** hacer update masivo a 0 sin confirmación explícita de Mauricio
- `recalcular()` y `recalcular_todos()` NO deben tocar esos campos — verificarlo
  antes de cualquier cambio en models.py de bonos
- Verificar antes y después de cualquier cambio:
  ```sql
  SELECT COUNT(*), SUM(bono_extra), SUM(ajuste_positivo)
  FROM bonos_produccion_bonoproduccionempleado bp
  JOIN bonos_produccion_configbonoperiodo cp ON bp.periodo_id=cp.id
  WHERE cp.mes=X AND cp.anio=Y AND (bono_extra>0 OR ajuste_positivo>0);
  ```

### Service Worker — caché PWA
Todo módulo que tenga un `sw.js` (actualmente: bonos_ventas, bonos_produccion) mantiene
un caché en el navegador del cliente. Cuando un deploy cambia HTML, CSS o JS visible
en esos módulos, el cliente **no verá el cambio** aunque el servidor ya lo tenga,
porque el SW sigue sirviendo la versión anterior.

Regla: **cualquier commit que modifique templates o estáticos de un módulo con SW debe
incluir en ese mismo commit un bump de la constante `CACHE_NAME` en su `sw.js`.**

- Cmd+Shift+R (hard-refresh) no bypasea el Service Worker — no es suficiente
- El bump de versión es la única garantía de que todos los clientes reciben la versión nueva
- Los archivos SW están en: `bonos_ventas/static/bonos_ventas/sw.js` y
  `bonos_produccion/static/bonos_produccion/sw.js`
- Después del deploy correr `collectstatic` para que el SW nuevo llegue a WhiteNoise

### Ramas desincronizadas
Si una rama local divergió: `git reset --hard origin/<rama>`

---

## Reglas absolutas — NO hacer sin confirmación de Mauricio
- Eliminar migraciones existentes
- Reset o drop de la base de datos
- Modificar .env en producción
- Push --force a main
- Eliminar archivos de modelos o urls
- Cambiar puertos en docker-compose.yml
- Alterar datos de nómina, RRHH, ventas o configuración operativa

---

## Usuarios operativos clave
| Usuario | Área | Rol crítico |
|---------|------|-------------|
| admin | DG | Acceso total |
| carolina.cayetano | Producción | Captura bono_extra y ajustes en bonos producción |
| johana.lopez | Ventas | Captura bono_extra y ajustes en bonos ventas |
| jorge.perez | Compras | — |
| paula.lugo | Capital Humano | Gestiona permisos y nómina |
| yesenia.soto | Administración | — |

---

## Apps Django y sus responsabilidades
```
core/             → Sucursales, Usuarios, AuditLog, Auth, Dashboard
maestros/         → Insumos, Proveedores, Unidades, Productos, Categorías
recetas/          → Recetas, Costeo, Matching, MRP, Plan Producción, Pronósticos
compras/          → Solicitudes, Órdenes de Compra, Recepciones, Presupuestos
inventario/       → Existencias, Movimientos, Ajustes, Aliases, Importación
activos/          → Equipos, Mantenimiento preventivo/correctivo
control/          → Discrepancias POS, Mermas, Captura móvil
crm/              → Clientes, Pedidos, Seguimiento multicanal
rrhh/             → Empleados, Nómina, Permisos, Vacantes
bonos_produccion/ → Bonos por área (Hornos, Embetunado, Producción, Logística)
bonos_ventas/     → Bonos de vendedoras y repartidores por sucursal
logistica/        → Rutas, Entregas
integraciones/    → API pública, POS Point, Logs
reportes/         → BI, Costeo, Consumo, Faltantes
api/              → Endpoints REST centralizados (80+)
horarios_especiales/ → Horarios por sucursal y fecha especial
orquestacion/     → Agentes IA, loops, memoria
```

## Views refactorizados (NO editar archivos originales — ya no existen)
- `api/views/` → 11 módulos: auth, maestros, recetas, inventario, integraciones,
  presupuestos, compras, produccion, ventas, activos, control
- `recetas/views/` → 5 módulos: recetas, matching, plan, reabasto, mrp

---

## Diseño Visual — CSS Variables
```css
--vino: #8B2252        /* Primario — sidebar, acentos, headers */
--vino-light: #A0365F  /* Hover de vino */
--dorado: #C9A84C      /* Acento dorado — badges, detalles premium */
--rosa: #F8E8EE        /* Fondo cards suave */
--blanco: #FFFAF5      /* Fondo general (cálido, NO blanco puro) */
--gris-suave: #F5F0EB  /* Fondo alternativo */
--texto: #3D2B2B       /* Texto principal (marrón oscuro) */
--texto-light: #7A6565 /* Texto secundario */
--verde: #4CAF50       /* Éxito */
--amarillo: #FFC107    /* Advertencia */
--rojo: #E53935        /* Error */
```
- Títulos (h1-h4): Playfair Display (serif) · Body/UI: Nunito (sans-serif)
- Sidebar fijo: 252px, gradiente vino (#8B2252 → #6B1740)
- NO usar frameworks JS externos en templates del ERP principal
- Los templates de bonos SÍ usan React 18 UMD (cargado desde unpkg)

---

## Verificación en navegador
Cuando la tarea afecte UI, flujos web, formularios, navegación o autenticación,
validar en navegador real antes de cerrar:
- Pantallas principales del módulo modificado
- Formularios: envío, validación, respuesta
- Errores de consola JavaScript
- Network requests relevantes (XHR/Fetch)
- Service worker y caché si algo no aparece

---

## Comandos útiles
```bash
python manage.py check                                        # Verificar proyecto
python manage.py migrate --check                              # Migraciones pendientes
python manage.py showmigrations bonos_produccion bonos_ventas # Estado migraciones bonos
python manage.py test bonos_produccion bonos_ventas rrhh --parallel
python manage.py runserver                                    # Servidor local
```

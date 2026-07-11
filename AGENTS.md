# AGENTS.md — Pollyana's Dolce ERP

## Contexto del proyecto
ERP operativo de Pollyana's Dolce, cadena de pastelerías en Sinaloa, México.
- 9 sucursales activas: Guasave (8) + Guamúchil (1)
- Stack: Django 5.0 + DRF + PostgreSQL + Redis + Celery
- Repo: github.com/maburgos12/pollyanas-dolce-erp (privado)
- Producción: https://erp.pollyanasdolce.com
- Rama principal: main

## Contexto de producto y diseño
Para cualquier tarea de UI, frontend, diseño visual, experiencia de usuario,
branding, tienda online o marketing, leer primero:
- `PRODUCT.md`: estrategia del producto, usuarios, principios, anti-referencias
  y accesibilidad.
- `DESIGN_STACK.md`: enrutamiento de skills entre `impeccable`, `hallmark`,
  `emil-design-eng` y los skills de Leon.

No duplicar las reglas completas de diseño en este archivo. `AGENTS.md` conserva
el protocolo operativo del ERP; `DESIGN_STACK.md` es la fuente compartida para
ejecución de diseño.

## Servidor de producción
- Host: 68.183.165.47
- Usuario: root
- Llave SSH: ~/.ssh/agente_dg_ops
- Directorio: /opt/pastelerias-erp
- Comando base: docker compose -f /opt/pastelerias-erp/docker-compose.yml

## Protocolo obligatorio antes de crear, modificar, implementar, mejorar o cambiar

Aplica para cualquier solicitud de Mauricio que implique tocar código, datos,
configuración, UI, permisos, navegación, reportes, jobs, integraciones,
prototipos o producción.

### 1. Clasificar el tipo de trabajo antes de tocar archivos
- **Consulta o diagnóstico:** solo leer, inspeccionar y reportar. No modificar
  nada salvo que Mauricio lo pida explícitamente.
- **Prototipo aislado:** mantenerlo separado del ERP real cuando sea posible. No
  usar Docker ni producción si no hace falta para validar la idea.
- **Prototipo dentro del ERP local:** usar rama limpia y Docker local solo como
  entorno de prueba. No presentarlo como validación de producción.
- **Implementación real:** trabajar en rama limpia, con checks, PR, deploy y
  validación en el lugar real donde se usa.

### 2. Sincronizar entorno local con main ANTES de tocar cualquier archivo

**Obligatorio al inicio de cada tarea, sin excepción.**
El SQLite local puede estar días o semanas detrás de `origin/main`. Si no se
sincroniza primero, `migrate --check` muestra migraciones pendientes de otras apps
que no son de la tarea, el entorno no es confiable y el diff final queda sucio.

```bash
git fetch origin main
git checkout -b codex/<modulo>-<descripcion> origin/main   # rama desde main actualizado
python manage.py migrate                                    # aplicar TODO lo que main tiene
python manage.py migrate --check                           # debe quedar en 0 pendientes
python manage.py check                                     # debe quedar en 0 errores
```

**Si `migrate --check` no es 0 al inicio:** aplicar las migraciones pendientes
antes de escribir código. Nunca iniciar una tarea sobre una DB local desactualizada.
Ese ruido de migraciones ajenas contamina el diff y obliga a trabajo extra al final.

### 3. Auditar estado del repo antes de cualquier cambio
Antes de editar archivos, ejecutar y revisar:
```bash
git status --short --branch
git diff --stat
```
Si se va a subir, integrar o basar trabajo en `main`, revisar también:
```bash
git branch -vv
git rev-list --left-right --count origin/main...HEAD
```

No continuar en esa rama si:
- hay cambios sin commitear que no pertenecen a la tarea;
- hay mezcla de módulos no relacionados;
- la rama no tiene upstream y se pretende subir;
- la rama está muy detrás de `origin/main` sin plan explícito de rebase/merge;
- el objetivo real no corresponde al nombre o historial de la rama.

En cualquiera de esos casos, detenerse, reportar el estado exacto y proponer
rescate: respaldar parche, crear rama limpia desde `origin/main` o desde la
rama correcta, y aplicar solo los cambios relacionados.

### 4. Una tarea, una rama, un objetivo
- No mezclar RRHH, recetas, ventas, reportes, CSS global, activos u otros módulos
  si la solicitud no los necesita.
- No hacer refactors oportunistas ni "aprovechar" para tocar archivos ajenos.
- El nombre de la rama debe reflejar la tarea: `codex/rrhh-permisos-jefe`,
  `codex/ventas-indicadores-prototipo`, etc.
- Para cambios grandes o productivos, crear rama nueva desde una base limpia
  antes de implementar.

### 5. Docker local no equivale a producción
- Docker Desktop puede usarse para prototipos o validación local del ERP.
- Si Docker local falla, diagnosticar primero logs, variables de entorno y
  `docker compose config`; no cambiar código ni producción por intuición.
- No modificar `.env` de producción ni puertos de `docker-compose.yml` sin
  confirmación explícita.
- Una validación local en Docker no sustituye la validación final en VPS,
  navegador real, reporte real, pantalla real o usuario real afectado.

### 6. Higiene de commits — quirúrgico y descriptivo
Antes de cualquier commit:
```bash
git status --short --branch
git log --oneline --decorate -5
git worktree list
```
- Confirmar **solo archivos relacionados con la tarea**. Si hay cambios ajenos,
  no mezclarlos: reportarlos y usar stash o rama separada.
- Nunca commitear:
  - `.DS_Store`, `.playwright-mcp/`, `.claude/`, `outputs/`, `output/`
  - `*.png`, `*.jpg`, `*.gif` fuera de `static/`
  - `storage/dg_reports/*.json`, `storage/dg_reports/*.md`
  - `storage/dg_reports/logs/*.log`
  - Logs, capturas, CSVs temporales, artefactos de runtime
- Mensaje de commit: descriptivo, en español o inglés técnico, que explique
  **qué** y **por qué**, no solo "fix".
- Si hay cambios ajenos sin commitear que no pertenecen a la tarea:
  ```bash
  git stash push -u -m "resguardo-<tarea>-<fecha>"
  ```

### 7. Pull requests — una tarea, un PR
Antes de crear cualquier PR:
```bash
git status --short --branch
git log --oneline --decorate -5
git worktree list
git diff origin/main..HEAD --stat
```
- Verificar que la rama **no mezcle tareas distintas**.
- Verificar que no haya pendientes sin confirmar en la rama.
- El título del PR debe reflejar exactamente qué cambia.
- No abrir PR si `python manage.py check` da errores.
- No abrir PR si hay migraciones sin verificar en producción.

### 8. Validación mínima antes de cerrar
- Correr `python manage.py check` antes de cualquier commit.
- Correr `python manage.py migrate --check` antes de deploy.
- Ejecutar tests del módulo afectado cuando existan.
- Para UI, permisos, navegación, PWA, formularios o flujos visibles, validar en
  navegador real con consola y Network/XHR cuando aplique.
- Para datos operativos, validar tabla/conteo/registros y luego confirmar que
  aparecen en la pantalla, reporte, app o archivo donde se usan.

### 9. Cierre responsable
No declarar terminado un cambio si solo compila, si solo responde la API, o si
solo la base de datos tiene datos. Termina hasta que el resultado esté validado
en el flujo real. Si no se puede validar, reportar el bloqueo exacto, lo que sí
quedó hecho y qué falta para confirmar.

## Lecciones críticas aprendidas en producción (NO ignorar)

### Migraciones — regla de oro
- Antes de cualquier PR que toque modelos, correr:
  ```bash
  python manage.py migrate --check
  python manage.py showmigrations bonos_produccion bonos_ventas rrhh
  ```
- Si los campos ya existen en producción pero la migración no está registrada,
  usar `--fake` para marcarla sin correrla. NUNCA borrar la migración.
- Un `AttributeError: object has no attribute 'campo'` en producción = migración
  no aplicada. Solución: `migrate --fake` + restart. No tocar código.

### Deploy — no es automático
El deploy NO se activa solo al mergear. Pasos obligatorios tras merge a main:
```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```
Si migrate falla por columna duplicada: usar `--fake` en la migración específica.
Usar `docker compose ... restart web` solo cuando cambie la imagen, dependencias del contenedor o variables de entorno; para cambios normales de Python/HTML/CSS/JS el `HUP` de Gunicorn evita la ventana de `502`.

### Service Worker — caché en módulos PWA
Cualquier módulo que registre un `sw.js` mantiene un caché activo en el navegador del
cliente. El cliente no recibe el cambio aunque el servidor ya lo tenga desplegado.
Cmd+Shift+R no bypasea el SW — no es suficiente.

**Regla general:** si un commit toca HTML, CSS o JS de un módulo que tiene `sw.js`,
ese mismo commit debe hacer bump del número de versión en `CACHE_NAME` dentro del SW.
Después del deploy correr `collectstatic` para que el archivo SW actualizado llegue al
servidor de estáticos (WhiteNoise).

Esto aplica a cualquier módulo con PWA hoy o en el futuro — no es exclusivo de bonos.

### Datos de usuarios — NUNCA pisar
- `bono_extra`, `ajuste_positivo`, `ajuste_negativo` son datos capturados por el
  equipo operativo. NUNCA resetearlos, sobreescribirlos ni incluirlos en seeds.
- `recalcular()` y `recalcular_todos()` NO deben tocar esos campos. Verificarlo
  antes de cualquier cambio en models.py de bonos.
- Si un endpoint llama `recalcular_todos()` automáticamente (ej: resumen), revisar
  que no pise datos manuales capturados por las jefas.

### PR abierto = tarea sin terminar — deploy obligatorio antes de continuar
Un PR mergeado sin deploy en VPS equivale a un cambio que no existe para el usuario.
No abrir una segunda tarea hasta que la primera esté deployada y validada en producción.

**Prohibido:**
- Crear PR y pasar a otra tarea sin deployar
- Copiar archivos al VPS por SCP como atajo al flujo git (git pull es el único canal)
- Declarar "listo" cuando el VPS aún corre el código anterior

**Flujo mínimo obligatorio al cerrar cualquier cambio de código:**
1. PR mergeado a main
2. `git pull origin main` en VPS
3. `bash scripts/deploy_web_safe.sh` en VPS
4. Verificar que el resultado es visible en producción
5. Borrar la rama de trabajo local y remota cuando ya esté mergeada, deployada
   y validada, para que no se atraviese ni aparezca como opción en otros hilos:
   `git branch -D <rama>` y `git push origin --delete <rama>`; después correr
   `git fetch --prune origin`.

### Ramas — control de desorden
- Una rama = una tarea = un módulo. Si la tarea crece, abrir rama nueva.
- Nunca dejar commits de fix+revert en la misma rama sin squashearlos.
- Antes de trabajar, confirmar con `git branch --show-current` que estás en la
  rama correcta. No asumir.
- Si una rama local está desincronizada de su origin: `git reset --hard origin/<rama>`.

### Permisos y datos operativos — contexto real
- Carolina Cayetano: jefa de producción. Captura datos en bonos producción.
- Johana López: jefa de ventas. Captura datos en bonos ventas.
- Sus capturas son datos reales de nómina. Un borrado accidental es crítico.
- Siempre verificar en producción que los datos existen antes y después de cualquier
  cambio que toque esos modelos:
  ```sql
  SELECT COUNT(*), SUM(bono_extra) FROM bonos_produccion_bonoproduccionempleado
  WHERE periodo_id IN (SELECT id FROM bonos_produccion_configbonoperiodo WHERE mes=X AND anio=Y);
  ```

## Reglas obligatorias antes de cualquier tarea

### NO hacer sin confirmación explícita de Mauricio:
- Eliminar migraciones existentes
- Hacer reset o drop de la base de datos
- Modificar .env en producción
- Hacer push a main directamente si el cambio es destructivo
- Eliminar archivos de modelos o urls
- Cambiar puertos en docker-compose.yml

### SIEMPRE hacer:
- Correr `python manage.py check` antes de cualquier commit
- Correr `python manage.py migrate --check` antes de deploy
- Usar ramas para cambios grandes: git checkout -b feature/nombre
- Commitear con mensajes descriptivos en español o inglés técnico
- Push a main solo cuando check da 0 errores

## Regla de cierre obligatorio

Ningún cambio, ajuste, corrección o carga de datos se considera terminado hasta
que el resultado final esté validado en el lugar real donde se usa. No cerrar
solo con "la API responde", "la base de datos tiene datos" o "los tests pasan"
si el usuario final necesita verlo o usarlo en una pantalla, app, reporte,
correo, archivo, PDF, Excel, endpoint, permiso o flujo operativo.

Flujo estándar para cambios de código:
1. Auditar primero el estado real: código, datos, logs, permisos, sesión,
   navegador o producción según aplique.
2. Crear rama nueva para el trabajo.
3. Hacer el cambio mínimo necesario, sin mezclar hilos ni tocar archivos/datos
   no relacionados.
4. Validar localmente o en el contenedor con checks, tests, lint o validación
   equivalente según el tipo de cambio.
5. Abrir PR, mergearlo y desplegar al ambiente correcto.
6. Ejecutar `migrate`, `collectstatic`, build o restart solo cuando aplique.
7. Validar el resultado final en producción o en el ambiente objetivo con
   evidencia concreta.
8. Si el cambio ya quedó mergeado, deployado y validado, borrar la rama local y
   remota de la tarea y podar referencias (`git fetch --prune origin`) para que
   no aparezca atravesada en otros hilos.

Para UI, PWA, permisos, navegación o flujos visibles, validar con navegador real
o con el usuario real afectado. Revisar también consola, Network/XHR, logs,
sesión, permisos, service worker y caché si algo no aparece.

Para datos operativos, validar el conteo y los registros en la tabla correcta,
y luego confirmar que aparecen en la pantalla, reporte o app donde se usan.
No alterar datos maestros, RRHH, nómina, ventas, inventario o configuración de
producción salvo que Mauricio lo pida explícitamente.

Si no se puede validar el resultado final, no dar la tarea por cerrada. Reportar
el bloqueo exacto, lo que sí quedó hecho y qué falta para confirmar.

## Arquitectura de módulos (14 apps Django)
| App | Responsabilidad |
|-----|----------------|
| core | Sucursales, usuarios, audit, auth |
| maestros | Insumos, proveedores, unidades, productos |
| recetas | Recetas, costeo, matching, MRP |
| inventario | Existencias, movimientos, ajustes |
| compras | Solicitudes, órdenes, recepciones |
| ventas | Histórico, pronóstico, solicitudes |
| produccion | Plan producción, forecast |
| crm | Clientes, pedidos, seguimiento |
| rrhh | Empleados, nómina |
| activos | Equipos, mantenimiento |
| reportes | BI, exports CSV/XLSX |
| control | Discrepancias POS vs producción |
| integraciones | API pública, POS PointMeUp, Google Business |
| horarios_especiales | Horarios por sucursal y fecha especial |
| orquestacion | Agentes IA, loops, memoria |

## Views refactorizados (NO editar archivos originales — ya no existen)
- api/views/ → 11 módulos: auth, maestros, recetas, inventario, integraciones, presupuestos, compras, produccion, ventas, activos, control
- recetas/views/ → 5 módulos: recetas, matching, plan, reabasto, mrp

## Usuarios del sistema
| Usuario | Área |
|---------|------|
| admin | DG — acceso total |
| carolina.cayetano | Producción |
| johana.lopez | Ventas |
| jorge.perez | Compras |
| paula.lugo | Capital Humano |
| yesenia.soto | Administración |

## Deploy en producción
```bash
cd /opt/pastelerias-erp
git pull origin main
docker compose restart web worker beat
docker compose exec -T web python manage.py migrate --noinput
```

## Backups
- Automático: diario 2am via cron
- Script: /opt/pastelerias-erp/scripts/backup_db.sh
- Destino: /opt/backups/erp/
- Retención: 7 días

## Otros sistemas en el mismo Droplet
| Sistema | Directorio | Puerto | Dominio |
|---------|-----------|--------|---------|
| Maya omnicanal | /opt/pollyana-omnichannel | 8003 | api.pollyanasdolce.com |
| ERP Django | /opt/pastelerias-erp | 8011 | erp.pollyanasdolce.com |
| ad_agent (próximo) | /opt/ad-agent | 8004 | ads.pollyanasdolce.com |

## Verificación en navegador

Cuando la tarea afecte UI, flujos web, formularios, navegación, autenticación
o integraciones visibles en navegador, usar **Chrome DevTools MCP** para validar
el comportamiento real antes de cerrar la tarea.

Aplica para:
- Cambios en templates HTML del ERP
- Nuevos endpoints o modificaciones a vistas Django
- Flujos de autenticación y permisos
- Integraciones con Point (scraping Playwright, sincronización)
- Cualquier otro sistema web trabajado desde Codex

### ERP — pruebas funcionales

Para cambios en módulos operativos del ERP, validar el flujo afectado en navegador
cuando sea posible, incluyendo:
- Pantallas principales del módulo modificado
- Formularios: envío, validación, respuesta
- Errores de consola JavaScript
- Network requests relevantes (XHR/Fetch)

Ejemplo de instrucción a Codex:
> "Valida el flujo en navegador antes de abrir el PR."
> "Abre el módulo X en Chrome DevTools y verifica que no hay errores de consola."

## Trabajo en paralelo por hilos

Estas reglas complementan las reglas existentes del proyecto. No sustituyen reglas de stack, deploy, pruebas, seguridad ni produccion.

- Usar `1 hilo = 1 branch = 1 worktree limpio`.
- Antes de empezar, revisar `git status --short --branch` y `git worktree list`.
- No trabajar sobre `main` ni sobre un checkout con cambios no relacionados.
- Si el arbol actual esta mezclado, abrir un worktree limpio desde `origin/main`.
- Nombrar cada rama con alcance real, por ejemplo: `codex/<modulo>-<cambio>`.
- Declarar al inicio del hilo: objetivo, alcance, archivos o carpetas permitidos, contratos compartidos afectados y si requiere deploy o solo validacion local.
- No editar archivos fuera del alcance declarado sin explicarlo primero.
- Si el cambio toca contratos compartidos como API, modelos, estado global, autenticacion, service worker, cache, variables de entorno, scripts de deploy o procesos compartidos, avisarlo antes de editar y validar consumidores afectados.
- Preview local, staging y produccion son evidencias distintas. No presentar validacion local como prueba de produccion.
- No desplegar desde una rama con cambios mezclados o no revisados.
- Si cambian las reglas de trabajo, actualizar `AGENTS.md` y `claude.md` en el mismo cambio para mantenerlas alineadas.
- Al cerrar un hilo: revisar el diff final, dejar estado limpio o documentado y limpiar ramas atoradas que puedan obstruir otros hilos.

## Acciones que cambian estado — contrato obligatorio

- Toda pantalla nueva con acciones como Guardar, Autorizar, Aprobar, Rechazar, Cancelar o Resolver debe mantener la posición y el contexto del usuario.
- Usar el contrato progresivo `data-async-action` y respuesta JSON/HTML compartida; nunca duplicar la lógica de negocio entre ambos formatos.
- Mostrar éxito, error, advertencia o información mediante el toast global accesible. El banner superior no puede ser la única respuesta inmediata.
- Bloquear únicamente el botón presionado, mostrar `Guardando…` o `Procesando…` y prevenir doble envío.
- Ante error, conservar inputs y habilitar reintento. Para POST tradicional, regresar a un identificador estable mediante fragmento.
- Usar confirmación modal solo para acciones destructivas o irreversibles; debe admitir Escape, atrapar foco y devolverlo al disparador.
- Registrar cada migración en `docs/ux/action-context-coverage.md`; no declarar cobertura total mientras existan pantallas pendientes.

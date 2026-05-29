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

## Deploy manual (NO es automático al mergear)
```bash
cd /opt/pastelerias-erp && git pull origin main
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web python manage.py migrate --noinput
docker compose -f /opt/pastelerias-erp/docker-compose.yml restart web
```
Si migrate falla por columna duplicada: `migrate <app> <numero> --fake` en la migración específica.

## Backups
- Automático: diario 2am via cron
- Script: /opt/pastelerias-erp/scripts/backup_db.sh · Destino: /opt/backups/erp/ · Retención: 7 días

## Otros sistemas en el mismo servidor
| Sistema | Directorio | Puerto | Dominio |
|---------|-----------|--------|---------|
| Maya omnicanal | /opt/pollyana-omnichannel | 8003 | api.pollyanasdolce.com |
| ERP Django | /opt/pastelerias-erp | 8011 | erp.pollyanasdolce.com |

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

### 2. Auditar estado del repo antes de cualquier cambio
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

### 3. Una tarea, una rama, un objetivo
- No mezclar RRHH + bonos + reportes + CSS + activos en una sola rama
- No hacer refactors oportunistas ni tocar archivos que no son de la tarea
- Nombre de rama: `codex/<modulo>-<descripcion>` o `fix/<descripcion>`
- Para cambios grandes o productivos, crear rama nueva desde base limpia
- Nunca dejar pares fix+revert sin squash — generan ruido y confusión

### 4. Docker local no equivale a producción
- Si Docker local falla, diagnosticar logs y variables de entorno primero;
  no cambiar código ni producción por intuición
- No modificar `.env` de producción ni puertos de `docker-compose.yml` sin
  confirmación explícita de Mauricio
- Una validación local no sustituye la validación final en VPS, navegador real,
  reporte real, pantalla real o usuario real afectado

### 5. Higiene de commits
- `git diff --stat` y `git diff` antes de commitear — solo archivos de la tarea
- No incluir `outputs/`, screenshots, logs, archivos temporales ni artefactos generados
- Si aparecen cambios ajenos durante el trabajo, preservarlos y no revertirlos;
  separar o pedir decisión si bloquean la tarea

### 6. Validación mínima antes de cerrar
- `python manage.py check` → 0 errores antes de cualquier commit
- `python manage.py migrate --check` → sin migraciones pendientes antes de deploy
- Tests del módulo afectado cuando existan
- Para UI, permisos, PWA, formularios o flujos visibles: validar en navegador real
  con consola y Network/XHR
- Para datos operativos: validar conteo/registros en tabla y confirmar que aparecen
  en la pantalla, reporte o app donde se usan

### 7. Cierre responsable
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

### Service Worker — caché PWA bonos
Después de deploy que cambia templates de bonos, los usuarios necesitan limpiar caché:
- Mac: Cmd+Shift+R · PC/Android: Ctrl+Shift+R
- No asumir que el deploy llegó al usuario sin confirmarlo

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

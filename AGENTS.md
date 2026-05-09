# AGENTS.md — Pollyana's Dolce ERP

## Contexto del proyecto
ERP operativo de Pollyana's Dolce, cadena de pastelerías en Sinaloa, México.
- 9 sucursales activas: Guasave (8) + Guamúchil (1)
- Stack: Django 5.0 + DRF + PostgreSQL + Redis + Celery
- Repo: github.com/maburgos12/pollyanas-dolce-erp (privado)
- Producción: https://erp.pollyanasdolce.com
- Rama principal: main

## Servidor de producción
- Host: 68.183.165.47
- Usuario: root
- Llave SSH: ~/.ssh/agente_dg_ops
- Directorio: /opt/pastelerias-erp
- Comando base: docker compose -f /opt/pastelerias-erp/docker-compose.yml

## Reglas obligatorias antes de cualquier tarea

### Protocolo de Git y ramas para cualquier IA
Antes de modificar archivos, Codex, Claude u otra IA debe leer este `AGENTS.md`
y aplicar el runbook [docs/OPERACION_GIT_AGENTES.md](docs/OPERACION_GIT_AGENTES.md).

Comandos obligatorios de arranque:
```bash
git status --short --branch
git log --oneline --decorate -5
```

Reglas de trabajo:
- La base normal para una tarea nueva es `main` actualizado, salvo que Mauricio
  indique explicitamente continuar una rama existente.
- Si el working tree tiene cambios sin confirmar, no iniciar una tarea nueva
  encima de esos cambios. Primero reportar rama actual, commits ahead/behind y
  archivos pendientes clasificados por modulo.
- No mezclar tareas en una misma rama. Una rama debe corresponder a un objetivo
  operativo concreto.
- No tocar archivos ya modificados por otra tarea si no pertenecen al alcance
  actual.
- No commitear capturas, logs, dumps de contexto, salidas Playwright/MCP ni
  archivos temporales. Guardarlos en `_archive/` o fuera del repo.
- Antes de cualquier commit, mostrar `git status --short` y confirmar que solo
  entran archivos del alcance.
- Para trabajo de produccion, el cierre correcto incluye commit, push, deploy en
  VPS y verificacion runtime cuando Mauricio lo pida o el cambio lo requiera.

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

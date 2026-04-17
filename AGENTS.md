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

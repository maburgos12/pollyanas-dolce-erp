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

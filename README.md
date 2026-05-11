# Pollyana's Dolce — ERP Operativo

Sistema integral de gestión empresarial para la operación de Pollyana's Dolce.
Administración, Ventas, Compras, Inventario, Producción, CRM, RRHH, Logística y Reportes.

## Módulos

| Módulo | Descripción |
|--------|-------------|
| Dashboard | Panel ejecutivo con KPIs operativos |
| Maestros | Insumos, proveedores, unidades de medida, productos |
| Recetas y Costeo | Gestión de recetas, costeo MP/MO/indirectos, versionado |
| Plan de Producción | MRP, pronósticos, plan desde forecast estadístico |
| Compras | Solicitudes, órdenes de compra, recepciones, workflow de aprobación |
| Inventario | Existencias, movimientos, ajustes, punto de reorden, importación almacén |
| CRM | Clientes, pedidos, seguimiento multicanal |
| Logística | Rutas y entregas |
| RRHH | Empleados y nómina |
| Activos | Equipos, mantenimiento preventivo/correctivo |
| Reportes | Costeo por receta, BI, exports CSV/XLSX |
| Control | Discrepancias ventas vs producción, captura móvil |
| Integraciones | API pública, POS Point, homologación |
| Auditoría | Bitácora completa de operaciones |

## Tecnología

- Django 5.0 + Django REST Framework
- PostgreSQL 16
- Docker + Railway
- API REST con 80+ endpoints

## Instalación rápida

### Paso 1: Requisitos

- Docker y Docker Compose instalados
- 4GB RAM disponible

### Paso 2: Descomprimir

```bash
unzip pastelerias_erp_sprint1.zip
cd pastelerias_erp
```

### Paso 3: Configurar

```bash
cp .env.example .env
# Los valores por defecto funcionan, no necesitas editarlo
```

URL local oficial: `http://localhost:8011`

### Paso 4: Levantar

```bash
docker compose up -d --build
```

### Paso 5: Inicializar

```bash
# Esperar 30 segundos a que inicie PostgreSQL
sleep 30

# Crear tablas
docker compose exec web python manage.py migrate

# Crear usuario admin
docker compose exec web python manage.py createsuperuser
# Usuario: admin
# Email: admin@pastelerias.mx
# Password: (el que quieras)
```

### Paso 6: Importar datos

```bash
docker compose exec web python manage.py import_costeo test_data/COSTEO_Prueba.xlsx
```

### Listo

Abre http://localhost:8011/admin

El servicio `web` corre con `runserver` dentro de Docker Compose y se recarga automáticamente cuando cambias código local.

**Usuario**: admin
**Password**: (el que pusiste)

### Ver reportes de importación

```bash
ls -lh logs/import_*
cat logs/import_summary_*.csv
```

### Probar API MRP

```bash
# Primero obtén un receta_id del admin
# Luego:
curl -X POST http://localhost:8011/api/mrp/explode/ \
  -H "Content-Type: application/json" \
  -d '{"receta_id": 1, "multiplicador": 5}'

# Requerimientos agregados por periodo (mes completo)
curl -X POST http://localhost:8011/api/mrp/calcular-requerimientos/ \
  -H "Content-Type: application/json" \
  -d '{"periodo":"2026-02","periodo_tipo":"mes"}'

# Crear plan de producción desde pronóstico mensual
curl -X POST http://localhost:8011/api/mrp/generar-plan-pronostico/ \
  -H "Content-Type: application/json" \
  -d '{"periodo":"2026-02","fecha_produccion":"2026-02-20","incluir_preparaciones":false}'
```

## API

La documentación completa de endpoints está en la sección API del sistema.
Base URL: `https://pollyanas-dolce-erp-production.up.railway.app/api/`

## Operación

- [Guía de rutina diaria](docs/OPERACION_RUTINA_DIARIA_ERP.md)
- [Plantilla operativa semanal](docs/PLANTILLA_OPERATIVA_DIARIA_SEMANAL.md)
- [ERP Doctor: auditoría local y producción solo lectura](docs/ERP_DOCTOR.md)
- [Operación de integraciones API](docs/OPERACION_INTEGRACIONES_API.md)
- [Operación de aliases](docs/OPERACION_ALIASES_API.md)
- [Arquitectura de orquestación Paperclip + ERP](docs/ARQUITECTURA_ORQUESTACION_PAPERCLIP_ERP.md)
- [Arquitectura de orquestador nativo del ERP](docs/ARQUITECTURA_ORQUESTADOR_NATIVO_ERP.md)
- [Mapa de capacidades del ERP a agentes](docs/MAPA_CAPACIDADES_ERP_A_AGENTES.md)
- [Análisis de políticas e indicadores para orquestación](docs/ANALISIS_POLITICAS_INDICADORES_ORQUESTACION.md)

## Validación UI local

Para evitar inestabilidad del perfil persistente de Chrome del MCP:

1. Limpia el perfil MCP cuando quede colgado:

```bash
./scripts/reset_mcp_chrome.sh
```

Reset completo del perfil MCP:

```bash
./scripts/reset_mcp_chrome.sh --hard
```

2. Valida pantallas en un contexto aislado, sin reutilizar el perfil del MCP:

```bash
UI_CHECK_USERNAME=tu_usuario UI_CHECK_PASSWORD=tu_password \
./scripts/validate_ui_local.sh --route "/recetas/?vista=productos" --expect-text "Pendientes operativos"
```

Artifacts:
- screenshot y HTML en `output/playwright/`
- usa un perfil temporal por corrida y no comparte sesión con Chrome/MCP

Wrapper automático recomendado:

```bash
UI_CHECK_USERNAME=tu_usuario UI_CHECK_PASSWORD=tu_password \
./scripts/ui_check_safe.sh --route "/recetas/?vista=productos" --expect-text "Pendientes operativos"
```

Qué hace:
- resetea primero el perfil MCP inestable
- luego valida la vista en un perfil temporal aislado

## Point: descarga grande y navegador operativo

Para bajar archivos grandes de Point sin depender de screenshots:

```bash
./.venv/bin/python manage.py download_point_file \
  --path "/Report/PrintReportes/" \
  --param "idreporte=3" \
  --param "ext=Excel"
```

El archivo queda en:
- `storage/pos_bridge/raw_exports/point_files/`

Para importar un historial de producto descargado desde Point a staging y reconciliación:

```bash
./.venv/bin/python manage.py import_point_product_history \
  --report-path "/ruta/al/archivo.xls"
```

Eso crea:
- staging de encabezado del archivo
- staging de movimientos por fila
- reconciliación básica contra la receta ERP resuelta

Para entrar a Point ya autenticado, buscar, llenar, hacer click y descargar desde la UI real:

```bash
./scripts/point_browser_pull.sh \
  --path "/Catalogos/Index" \
  --headed \
  --wait-text "Catálogos"
```

Ejemplo con llenado, click y descarga:

```bash
./scripts/point_browser_pull.sh \
  --path "/Reportes/VentasCategorias" \
  --fill "#datepicker=2026-03-01" \
  --fill "#datepicker2=2026-03-29" \
  --click "#btnBuscar" \
  --download-selector "#btnExcel" \
  --download-name "ventas_categorias_marzo.xls"
```

Artifacts:
- navegador Point: `output/point_browser/`
- descargas HTTP grandes: `storage/pos_bridge/raw_exports/point_files/`

## Costos operativos y pricing mensual

Bootstrap del catálogo base:

```bash
./.venv/bin/python manage.py bootstrap_operating_finance
```

Exportar plantilla XLSX para cargar gastos:

```bash
./.venv/bin/python manage.py export_operating_finance_template
```

La plantilla se genera en:
- `output/spreadsheet/operating_finance_expenses_template.xlsx`

Importar gastos mensuales desde la plantilla:

```bash
./.venv/bin/python manage.py import_operating_finance_expenses \
  --file "output/spreadsheet/operating_finance_expenses_template.xlsx"
```

Regenerar snapshot mensual de costo, contribución y pricing:

```bash
./.venv/bin/python manage.py snapshot_operating_finance --period 2026-03
```

Notas operativas:
- `external_key` debe ser única por fila para permitir reimportación sin duplicados.
- `periodo` va en formato `YYYY-MM`.
- `centro_costo` y `categoria_gasto` deben tomarse de la hoja `Catalogos`.

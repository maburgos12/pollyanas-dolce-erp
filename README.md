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

### Paso 4: Levantar

```bash
docker-compose up -d --build
```

### Paso 5: Inicializar

```bash
# Esperar 30 segundos a que inicie PostgreSQL
sleep 30

# Crear tablas
docker-compose exec web python manage.py migrate

# Crear usuario admin
docker-compose exec web python manage.py createsuperuser
# Usuario: admin
# Email: admin@pastelerias.mx
# Password: (el que quieras)
```

### Paso 6: Importar datos

```bash
docker-compose exec web python manage.py import_costeo test_data/COSTEO_Prueba.xlsx
```

### Listo

Abre http://localhost:8000/admin

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
curl -X POST http://localhost:8000/api/mrp/explode/ \
  -H "Content-Type: application/json" \
  -d '{"receta_id": 1, "multiplicador": 5}'

# Requerimientos agregados por periodo (mes completo)
curl -X POST http://localhost:8000/api/mrp/calcular-requerimientos/ \
  -H "Content-Type: application/json" \
  -d '{"periodo":"2026-02","periodo_tipo":"mes"}'

# Crear plan de producción desde pronóstico mensual
curl -X POST http://localhost:8000/api/mrp/generar-plan-pronostico/ \
  -H "Content-Type: application/json" \
  -d '{"periodo":"2026-02","fecha_produccion":"2026-02-20","incluir_preparaciones":false}'
```

## API

La documentación completa de endpoints está en la sección API del sistema.
Base URL: `https://pollyanas-dolce-erp-production.up.railway.app/api/`

## Operación

- [Guía de rutina diaria](docs/OPERACION_RUTINA_DIARIA_ERP.md)
- [Plantilla operativa semanal](docs/PLANTILLA_OPERATIVA_DIARIA_SEMANAL.md)
- [Operación de integraciones API](docs/OPERACION_INTEGRACIONES_API.md)
- [Operación de aliases](docs/OPERACION_ALIASES_API.md)

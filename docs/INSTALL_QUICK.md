# 🚀 INSTALACIÓN RÁPIDA - 5 MINUTOS

## Paso 1: Requisitos
- Docker y Docker Compose instalados
- 4GB RAM disponible

## Paso 2: Descomprimir
```bash
unzip pastelerias_erp_sprint1.zip
cd pastelerias_erp
```

## Paso 3: Configurar
```bash
cp .env.example .env
# Los valores por defecto funcionan, no necesitas editarlo
```

URL local oficial: `http://localhost:8011`

## Paso 4: Levantar
```bash
docker compose up -d --build
```

## Paso 5: Inicializar
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

## Paso 6: Importar datos
```bash
docker compose exec web python manage.py import_costeo test_data/COSTEO_Prueba.xlsx
```

## ✅ Listo!

Abre http://localhost:8011/admin

El servicio `web` se recarga automáticamente al detectar cambios de código local.

**Usuario**: admin  
**Password**: (el que pusiste)

## Ver reportes de importación
```bash
ls -lh logs/import_*
cat logs/import_summary_*.csv
```

## Probar API MRP
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

---

Ver **README.md** para documentación completa.

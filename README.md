# Mini-ERP Pollyana's Dolce (Sprint 1 ejecutable)

Sprint 1 entrega:
- Importación desde Excel: catálogo de costos (Costo Materia Prima) + recetas (hojas con “Ingredientes”)
- Matching de insumos (EXACT / CONTAINS / FUZZY) con cola “Needs review”
- UI web (Django) para ver recetas, detalle, pendientes y MRP básico
- API: POST /api/mrp/explode/
- API: POST /api/mrp/calcular-requerimientos/ (por plan, por periodo o por lista manual)

## Requisitos
- Docker Desktop (Mac/Windows) **o** Python 3.12 + Postgres 16

## Opción A (recomendada): correr con Docker
1) Copia `.env.example` a `.env`
2) En la carpeta del proyecto:
   ```bash
   docker compose up --build
   ```
3) En otra terminal:
   ```bash
   docker compose exec web python manage.py migrate
   docker compose exec web python manage.py createsuperuser
   docker compose exec web python manage.py bootstrap_roles
   docker compose exec web python manage.py import_costeo test_data/COSTEO_Prueba.xlsx
   ```
4) Abre:
   - UI: http://localhost:8000/
   - Admin: http://localhost:8000/admin/

## Opción B: correr sin Docker (dev)
- Configura un Postgres local y variables de entorno similares a `.env.example`
- Instala dependencias:
  ```bash
  pip install -r requirements.txt
  python manage.py migrate
  python manage.py createsuperuser
  python manage.py bootstrap_roles
  python manage.py runserver
  ```

## Archivos importantes
- `recetas/management/commands/import_costeo.py` (comando de importación)
- `recetas/utils/importador.py` (parser de Excel)
- `recetas/utils/matching.py` (matching)
- `logs/` (reportes CSV del import)

## Notas
- El matching y captura operativa en UI ya incluyen búsqueda/autocomplete para insumos.
- El importador es idempotente por `source_hash` en costos y por `hash_contenido` en recetas.

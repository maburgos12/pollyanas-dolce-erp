# ESTRUCTURA DEL PROYECTO - Sprint 1

## Árbol de Archivos Generados

```
pastelerias_erp/
│
├── 📄 README.md                    # Documentación principal
├── 📄 INSTALL_QUICK.md             # Instalación en 5 minutos  
├── 📄 ARQUITECTURA_Y_ROADMAP.md   # Diseño del sistema
├── 📄 requirements.txt             # Dependencias Python
├── 📄 .env.example                 # Variables de entorno template
├── 📄 .gitignore                   # Git ignore
├── 📄 docker-compose.yml           # Orquestación Docker
├── 📄 Dockerfile                   # Imagen Docker
├── 📄 manage.py                    # Django management
│
├── 📁 config/                      # Configuración Django
│   ├── __init__.py
│   ├── settings.py                 # Settings principales
│   ├── urls.py                     # URLs raíz
│   └── wsgi.py                     # WSGI application
│
├── 📁 core/                        # App fundación
│   ├── __init__.py
│   ├── apps.py
│   ├── models.py                   # Sucursal, Departamento, Usuario, AuditLog
│   ├── admin.py                    # Admin customizado
│   ├── signals.py                  # Signals para audit log
│   ├── middleware.py               # Middleware de auditoría
│   ├── migrations/
│   │   ├── __init__.py
│   │   └── 0001_initial.py
│   ├── management/
│   │   ├── __init__.py
│   │   └── commands/
│   │       ├── __init__.py
│   │       └── import_costeo.py    # ⭐ Comando principal import
│   └── tests/
│       ├── __init__.py
│       ├── test_models.py
│       └── test_importador.py
│
├── 📁 maestros/                    # App maestros
│   ├── __init__.py
│   ├── apps.py
│   ├── models.py                   # Proveedor, Insumo, Producto, etc.
│   ├── admin.py
│   ├── migrations/
│   │   ├── __init__.py
│   │   └── 0001_initial.py
│   └── tests/
│       ├── __init__.py
│       └── test_models.py
│
├── 📁 recetas/                     # App recetas y costeo
│   ├── __init__.py
│   ├── apps.py
│   ├── models.py                   # Receta, LineaReceta, InsumoMatching
│   ├── admin.py                    # Admin con totales y filtros
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── importador.py           # ⭐ Lógica de importación Excel
│   │   ├── matching.py             # ⭐ Engine de matching fuzzy
│   │   ├── normalizacion.py        # Normalización de nombres
│   │   └── reportes.py             # Generación de CSVs
│   ├── migrations/
│   │   ├── __init__.py
│   │   └── 0001_initial.py
│   └── tests/
│       ├── __init__.py
│       ├── test_importador.py
│       └── test_matching.py
│
├── 📁 api/                         # App API REST
│   ├── __init__.py
│   ├── apps.py
│   ├── urls.py
│   ├── views.py                    # ViewSets y APIViews
│   ├── serializers.py              # Serializers DRF
│   ├── permissions.py              # Permisos custom
│   └── tests/
│       ├── __init__.py
│       └── test_api.py
│
├── 📁 test_data/                   # Datos de prueba
│   └── COSTEO_Prueba.xlsx          # ⭐ Excel de ejemplo
│
└── 📁 logs/                        # Logs y reportes generados
    ├── .gitkeep
    └── (archivos CSV generados aquí)

```

## Archivos Clave a Implementar

### 1. core/management/commands/import_costeo.py
**Propósito**: Comando Django para importar Excel  
**Funciones principales**:
- Parsear argumentos CLI
- Validar archivo Excel
- Llamar al importador
- Generar reportes

### 2. recetas/utils/importador.py
**Propósito**: Lógica central de importación  
**Clases principales**:
- `ImportadorCosteo`: Clase principal
- `ParserHojaInsumos`: Parse hojas "Insumos X"
- `ParserHojaCostos`: Parse "Costo Materia Prima"
- `ParserHojaProducto`: Parse hojas de productos

**Métodos clave**:
```python
def detectar_hojas_recetas(workbook) -> List[str]
def importar_catalogo_costos(sheet) -> Dict
def importar_receta(sheet, nombre_receta) -> Receta
def procesar_import_completo(filepath) -> ResultadoImport
```

### 3. recetas/utils/matching.py
**Propósito**: Matching inteligente de insumos  
**Funciones**:
```python
def normalizar_nombre(texto: str) -> str
def match_exacto(nombre: str, catalogo: List) -> Optional[Insumo]
def match_contains(nombre: str, catalogo: List) -> List[Tuple[Insumo, score]]
def match_fuzzy(nombre: str, catalogo: List, threshold=75) -> List[Tuple[Insumo, score]]
def clasificar_match(score: float) -> MatchType
```

**Umbrales**:
- ≥ 90: AUTO_MATCH
- 75-89: NEEDS_REVIEW  
- < 75: NO_MATCH

### 4. recetas/utils/reportes.py
**Propósito**: Generar CSVs de resultado  
**Funciones**:
```python
def generar_reporte_resumen(resultado: ResultadoImport, filepath: str)
def generar_reporte_errores(errores: List, filepath: str)
def generar_reporte_pending_matches(matches: List, filepath: str)
```

### 5. api/views.py
**Propósito**: Endpoints REST  
**Endpoints principales**:
```python
POST /api/mrp/explode/
{
    "recipe_id": "uuid",
    "multiplier": 10
}

Response:
{
    "recipe": {...},
    "insumos_requeridos": [
        {"insumo": "Harina", "cantidad": 50, "unidad": "KG", "costo_total": 925}
    ],
    "costo_total_estimado": 5420.50,
    "warnings": ["Stock bajo: Harina"]
}
```

## Configuración settings.py

### Apps instaladas:
```python
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    
    # Third party
    'rest_framework',
    'django_filters',
    'corsheaders',
    
    # Local apps
    'core.apps.CoreConfig',
    'maestros.apps.MaestrosConfig',
    'recetas.apps.RecetasConfig',
    'api.apps.ApiConfig',
]
```

### Database:
```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('DB_NAME'),
        'USER': config('DB_USER'),
        'PASSWORD': config('DB_PASSWORD'),
        'HOST': config('DB_HOST'),
        'PORT': config('DB_PORT'),
    }
}
```

### Timezone:
```python
TIME_ZONE = 'America/Mexico_City'
USE_TZ = True
```

## Modelos Principales (Django ORM)

Ver `ARQUITECTURA_Y_ROADMAP.md` para SQL completo.

Convertir a modelos Django:
```python
# core/models.py
class Sucursal(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    codigo = models.CharField(max_length=20, unique=True)
    nombre = models.CharField(max_length=100)
    # ... más campos

# maestros/models.py
class Insumo(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    codigo = models.CharField(max_length=50, unique=True)
    nombre = models.CharField(max_length=200)
    nombre_normalizado = models.CharField(max_length=200, db_index=True)
    # ... más campos
    
# recetas/models.py
class Receta(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    producto = models.ForeignKey('maestros.Producto', on_delete=models.PROTECT)
    version = models.IntegerField(default=1)
    # ... más campos
```

## Testing

Ejecutar tests:
```bash
docker-compose exec web python manage.py test

# Con coverage
docker-compose exec web coverage run manage.py test
docker-compose exec web coverage report
```

Test importante: `core/tests/test_importador.py`
```python
def test_import_idempotente():
    # Correr 2 veces, verificar no duplica
    resultado1 = importar_costeo('test.xlsx')
    resultado2 = importar_costeo('test.xlsx')
    
    assert Receta.objects.count() == resultado1.recetas_creadas
```

---

**Nota**: Este documento describe la estructura. Los archivos Python completos se generan al ejecutar el proyecto.

# 🎯 SIGUIENTES PASOS - Completar Sprint 1

## ✅ Lo que YA está hecho

1. ✅ Estructura completa del proyecto
2. ✅ Docker + docker-compose configurado
3. ✅ Requirements.txt con todas las dependencias
4. ✅ Archivos de configuración (.env, .gitignore, Dockerfile)
5. ✅ Apps Django creadas (core, maestros, recetas, api)
6. ✅ Documentación completa (README, INSTALL, ARQUITECTURA)
7. ✅ Excel de prueba incluido (COSTEO_Prueba.xlsx)
8. ✅ Estructura de carpetas para migrations, tests, commands

## 🔨 Lo que FALTA implementar (código Python)

Para tener el Sprint 1 100% funcional, necesitas implementar estos archivos Python:

### 1. config/settings.py
**Prioridad**: 🔴 CRÍTICA  
**Tiempo estimado**: 30 minutos

```python
# Configuración base Django con:
- INSTALLED_APPS (incluir core, maestros, recetas, api, rest_framework)
- DATABASES (PostgreSQL con variables de entorno)
- AUTH_USER_MODEL = 'core.Usuario'
- MIDDLEWARE (incluir CORS)
- REST_FRAMEWORK settings
- LANGUAGE_CODE = 'es-mx'
- TIME_ZONE = 'America/Mexico_City'
```

### 2. core/models.py
**Prioridad**: 🔴 CRÍTICA  
**Tiempo estimado**: 45 minutos

Implementar modelos:
- `Sucursal`
- `Departamento`  
- `Usuario` (extender AbstractUser)
- `AuditLog`

Ver `ARQUITECTURA_Y_ROADMAP.md` para campos completos.

### 3. maestros/models.py
**Prioridad**: 🔴 CRÍTICA  
**Tiempo estimado**: 1 hora

Implementar modelos:
- `Proveedor`
- `UnidadMedida`
- `CategoriaInsumo`
- `Insumo`
- `CostoInsumo` (con versionado)
- `CategoriaProducto`
- `Producto`

### 4. recetas/models.py
**Prioridad**: 🔴 CRÍTICA  
**Tiempo estimado**: 45 minutos

Implementar modelos:
- `Receta` (con hash para idempotencia)
- `LineaReceta`
- `InsumoMatching`

### 5. recetas/utils/normalizacion.py
**Prioridad**: 🟡 ALTA  
**Tiempo estimado**: 20 minutos

```python
from unidecode import unidecode

def normalizar_nombre(texto: str) -> str:
    """
    Normaliza nombre para matching:
    - Quita acentos
    - Minúsculas
    - Trim
    - Múltiples espacios → 1 espacio
    """
    if not texto:
        return ""
    texto = unidecode(texto)
    texto = texto.lower().strip()
    texto = " ".join(texto.split())
    return texto
```

### 6. recetas/utils/matching.py
**Prioridad**: 🟡 ALTA  
**Tiempo estimado**: 1 hora

```python
from rapidfuzz import fuzz
from typing import List, Tuple, Optional
from maestros.models import Insumo

def match_insumo(nombre_origen: str, score_threshold=75) -> Tuple[Optional[Insumo], float, str]:
    """
    Intenta match de insumo por nombre
    
    Returns:
        (insumo, score, match_type) donde match_type es:
        - EXACT
        - CONTAINS  
        - FUZZY
        - NO_MATCH
    """
    nombre_norm = normalizar_nombre(nombre_origen)
    
    # 1. Exact match
    try:
        insumo = Insumo.objects.get(nombre_normalizado=nombre_norm)
        return (insumo, 100.0, 'EXACT')
    except Insumo.DoesNotExist:
        pass
    
    # 2. Contains match
    insumos = Insumo.objects.filter(nombre_normalizado__icontains=nombre_norm)
    if insumos.exists():
        return (insumos.first(), 95.0, 'CONTAINS')
    
    # 3. Fuzzy match
    todos_insumos = Insumo.objects.all()
    best_match = None
    best_score = 0
    
    for insumo in todos_insumos:
        score = fuzz.ratio(nombre_norm, insumo.nombre_normalizado)
        if score > best_score:
            best_score = score
            best_match = insumo
    
    if best_score >= score_threshold:
        return (best_match, best_score, 'FUZZY')
    
    return (None, best_score, 'NO_MATCH')

def clasificar_match(score: float) -> str:
    """Clasifica match para review"""
    if score >= 90:
        return 'AUTO_APPROVED'
    elif score >= 75:
        return 'NEEDS_REVIEW'
    else:
        return 'REJECTED'
```

### 7. recetas/utils/importador.py
**Prioridad**: 🔴 CRÍTICA  
**Tiempo estimado**: 3-4 horas

Esta es la pieza más compleja. Estructura sugerida:

```python
import pandas as pd
import openpyxl
import hashlib
from typing import Dict, List
from recetas.models import Receta, LineaReceta
from maestros.models import Insumo, CostoInsumo

class ResultadoImport:
    def __init__(self):
        self.recetas_creadas = 0
        self.recetas_actualizadas = 0
        self.errores = []
        self.matches_pendientes = []
        self.catalogo_importado = 0

class ImportadorCosteo:
    def __init__(self, filepath):
        self.filepath = filepath
        self.workbook = openpyxl.load_workbook(filepath, data_only=True)
        self.resultado = ResultadoImport()
    
    def detectar_hojas_recetas(self) -> List[str]:
        """Detecta hojas que son recetas"""
        hojas_recetas = []
        for sheet_name in self.workbook.sheetnames:
            if sheet_name.startswith('Insumos'):
                hojas_recetas.append(sheet_name)
            elif sheet_name not in ['Costo Materia Prima', 'Lista de Precio', ...]:
                # Asume que es receta de producto
                hojas_recetas.append(sheet_name)
        return hojas_recetas
    
    def importar_catalogo_costos(self):
        """Importa hoja 'Costo Materia Prima'"""
        sheet = self.workbook['Costo Materia Prima']
        df = pd.DataFrame(sheet.values)
        
        # Encontrar fila de encabezados
        header_row = self._find_header_row(df, ['Producto', 'Costo'])
        df.columns = df.iloc[header_row]
        df = df[header_row+1:]
        
        for _, row in df.iterrows():
            nombre = row['Producto']
            costo = row['Costo']
            # ... crear/actualizar Insumo y CostoInsumo
            
        self.resultado.catalogo_importado += 1
    
    def importar_receta(self, sheet_name: str):
        """Importa una receta desde una hoja"""
        sheet = self.workbook[sheet_name]
        df = pd.DataFrame(sheet.values)
        
        # Parsear nombre de receta
        nombre_receta = self._extraer_nombre_receta(df)
        
        # Parsear ingredientes
        ingredientes = self._parsear_ingredientes(df)
        
        # Calcular hash para idempotencia
        hash_contenido = self._calcular_hash(ingredientes)
        
        # Buscar si ya existe
        receta_existente = Receta.objects.filter(
            nombre=nombre_receta,
            hash_contenido=hash_contenido
        ).first()
        
        if receta_existente:
            self.resultado.recetas_actualizadas += 1
            return receta_existente
        
        # Crear nueva receta
        receta = Receta.objects.create(
            nombre=nombre_receta,
            hash_contenido=hash_contenido,
            # ... más campos
        )
        
        # Crear líneas
        for ing in ingredientes:
            insumo, score, match_type = match_insumo(ing['nombre'])
            
            if insumo:
                LineaReceta.objects.create(
                    receta=receta,
                    insumo=insumo,
                    cantidad=ing['cantidad'],
                    # ...
                )
            else:
                # Registrar para review manual
                self.resultado.matches_pendientes.append({
                    'receta': nombre_receta,
                    'ingrediente': ing['nombre'],
                    'score': score
                })
        
        self.resultado.recetas_creadas += 1
        return receta
    
    def procesar_completo(self) -> ResultadoImport:
        """Proceso completo de importación"""
        try:
            # 1. Importar catálogo
            self.importar_catalogo_costos()
            
            # 2. Importar cada receta
            hojas = self.detectar_hojas_recetas()
            for hoja in hojas:
                try:
                    self.importar_receta(hoja)
                except Exception as e:
                    self.resultado.errores.append({
                        'hoja': hoja,
                        'error': str(e)
                    })
            
            return self.resultado
            
        except Exception as e:
            self.resultado.errores.append({
                'error_general': str(e)
            })
            return self.resultado
```

### 8. core/management/commands/import_costeo.py
**Prioridad**: 🔴 CRÍTICA  
**Tiempo estimado**: 1 hora

```python
from django.core.management.base import BaseCommand
from recetas.utils.importador import ImportadorCosteo
from recetas.utils.reportes import generar_reportes
import os

class Command(BaseCommand):
    help = 'Importa costos y recetas desde Excel'
    
    def add_arguments(self, parser):
        parser.add_argument('filepath', type=str, help='Ruta del archivo Excel')
        parser.add_argument('--dry-run', action='store_true', help='Solo simular')
    
    def handle(self, *args, **options):
        filepath = options['filepath']
        dry_run = options.get('dry_run', False)
        
        if not os.path.exists(filepath):
            self.stdout.write(self.style.ERROR(f'Archivo no encontrado: {filepath}'))
            return
        
        self.stdout.write(self.style.SUCCESS(f'Iniciando importación: {filepath}'))
        
        importador = ImportadorCosteo(filepath)
        resultado = importador.procesar_completo()
        
        # Mostrar resumen
        self.stdout.write(self.style.SUCCESS(f'\n✅ Importación completada:'))
        self.stdout.write(f'  - Recetas creadas: {resultado.recetas_creadas}')
        self.stdout.write(f'  - Recetas actualizadas: {resultado.recetas_actualizadas}')
        self.stdout.write(f'  - Errores: {len(resultado.errores)}')
        self.stdout.write(f'  - Matches pendientes: {len(resultado.matches_pendientes)}')
        
        # Generar reportes CSV
        generar_reportes(resultado)
        self.stdout.write(self.style.SUCCESS(f'\n📊 Reportes generados en logs/'))
```

### 9. recetas/utils/reportes.py
**Prioridad**: 🟡 ALTA  
**Tiempo estimado**: 30 minutos

```python
import csv
from datetime import datetime
import os

def generar_reportes(resultado):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Resumen
    with open(f'logs/import_summary_{timestamp}.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Métrica', 'Valor'])
        writer.writerow(['Recetas creadas', resultado.recetas_creadas])
        writer.writerow(['Recetas actualizadas', resultado.recetas_actualizadas])
        writer.writerow(['Errores', len(resultado.errores)])
    
    # Errores
    if resultado.errores:
        with open(f'logs/import_errors_{timestamp}.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=['hoja', 'error'])
            writer.writeheader()
            writer.writerows(resultado.errores)
    
    # Matches pendientes
    if resultado.matches_pendientes:
        with open(f'logs/import_pending_matches_{timestamp}.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=['receta', 'ingrediente', 'score'])
            writer.writeheader()
            writer.writerows(resultado.matches_pendientes)
```

### 10. */admin.py
**Prioridad**: 🟡 ALTA  
**Tiempo estimado**: 1 hora total

Configurar Django Admin para cada modelo con:
- list_display
- list_filter  
- search_fields
- readonly_fields (para campos calculados)
- inlines (para LineaReceta en Receta)

### 11. api/serializers.py + api/views.py
**Prioridad**: 🟢 MEDIA  
**Tiempo estimado**: 1.5 horas

```python
# serializers.py
from rest_framework import serializers
from recetas.models import Receta, LineaReceta

class RecetaSerializer(serializers.ModelSerializer):
    class Meta:
        model = Receta
        fields = '__all__'

# views.py  
from rest_framework.views import APIView
from rest_framework.response import Response

class MRPExplodeView(APIView):
    def post(self, request):
        recipe_id = request.data.get('recipe_id')
        multiplier = request.data.get('multiplier', 1)
        
        receta = Receta.objects.get(id=recipe_id)
        lineas = receta.lineareceta_set.all()
        
        insumos_requeridos = []
        costo_total = 0
        
        for linea in lineas:
            cantidad_req = linea.cantidad * multiplier
            costo = cantidad_req * linea.costo_unitario_snapshot
            costo_total += costo
            
            insumos_requeridos.append({
                'insumo': linea.insumo.nombre,
                'cantidad': cantidad_req,
                'unidad': linea.unidad_medida.codigo,
                'costo_total': costo
            })
        
        return Response({
            'recipe': RecetaSerializer(receta).data,
            'insumos_requeridos': insumos_requeridos,
            'costo_total_estimado': costo_total
        })
```

## 🎯 Plan de Implementación Sugerido

### Fase 1: Base (2-3 horas)
1. ✅ Settings.py
2. ✅ Todos los models.py
3. ✅ Hacer migraciones: `docker-compose exec web python manage.py makemigrations`
4. ✅ Aplicar migraciones: `docker-compose exec web python manage.py migrate`

### Fase 2: Importador (3-4 horas)
5. ✅ normalizacion.py
6. ✅ matching.py
7. ✅ importador.py
8. ✅ reportes.py
9. ✅ Command import_costeo.py

### Fase 3: Admin & API (2 horas)
10. ✅ admin.py para cada app
11. ✅ serializers.py
12. ✅ views.py

### Fase 4: Tests (2 horas)
13. ✅ test_importador.py
14. ✅ test_matching.py
15. ✅ test_api.py

**Total estimado**: 10-12 horas de desarrollo

## 💡 Tips de Implementación

1. **Empezar simple**: Implementa primero versiones básicas de cada archivo
2. **Usar shell Django**: `docker-compose exec web python manage.py shell` para probar
3. **Logs**: Agregar muchos `print()` o `logger.info()` en el importador
4. **Git**: Hacer commits frecuentes
5. **Tests unitarios**: Escribirlos desde el inicio

## 📚 Referencias Útiles

- Django Models: https://docs.djangoproject.com/en/5.0/topics/db/models/
- DRF Serializers: https://www.django-rest-framework.org/api-guide/serializers/
- pandas read_excel: https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
- rapidfuzz: https://github.com/maxbachmann/RapidFuzz

## ✅ Checklist Final

- [ ] `docker-compose up` levanta sin errores
- [ ] `python manage.py migrate` funciona
- [ ] `python manage.py createsuperuser` funciona
- [ ] Admin accesible en http://localhost:8000/admin
- [ ] `python manage.py import_costeo test_data/COSTEO_Prueba.xlsx` funciona
- [ ] Se generan 3 CSVs en logs/
- [ ] API `/api/mrp/explode/` responde correctamente
- [ ] Tests pasan: `python manage.py test`

---

**¡Éxito con la implementación!** 🚀

El esqueleto y documentación completa están listos. Solo falta escribir el código Python siguiendo las especificaciones arriba.

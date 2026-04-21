# TASK: Implementar módulo de rentabilidad por sucursal

## Contexto del proyecto
- Repo: `pollyanas-dolce-erp` (Django 5.0 + DRF + PostgreSQL + Docker + Railway)
- App destino: `sucursales` (ya existe)
- Brand ERP: vino `#8B2252`, oro `#C9A84C`, fuentes Playfair Display + Nunito
- Python 3.11+, Django 5.0, Celery + Redis ya configurados

## Objetivo
Agregar un módulo completo de análisis de rentabilidad por sucursal al ERP existente.
**No modificar ningún archivo existente** excepto los indicados en "Archivos a modificar".

---

## Archivos a CREAR (contenido adjunto abajo)

### 1. `sucursales/models_rentabilidad.py`
Pegar el contenido del archivo `models.py` adjunto tal cual.

### 2. `sucursales/agente_rentabilidad.py`
Pegar el contenido del archivo `agente.py` adjunto tal cual.

### 3. `sucursales/views_rentabilidad.py`
Pegar el contenido del archivo `views.py` adjunto tal cual.

### 4. `sucursales/templates/sucursales/rentabilidad_dashboard.html`
Pegar el contenido del archivo `rentabilidad_dashboard.html` adjunto tal cual.
Crear el directorio si no existe.

### 5. `sucursales/admin_rentabilidad.py`
Pegar el contenido del archivo `admin.py` adjunto tal cual.

---

## Archivos a MODIFICAR

### `sucursales/models.py`
Agregar al final del archivo (sin tocar nada de lo que ya existe):
```python
# --- Módulo de rentabilidad ---
from .models_rentabilidad import SucursalRentabilidad, EstadoRentabilidad
```

### `sucursales/admin.py`
Agregar al final del archivo:
```python
from .admin_rentabilidad import SucursalRentabilidadAdmin
admin.site.register(SucursalRentabilidad, SucursalRentabilidadAdmin)
```
Y agregar en los imports del tope si no está:
```python
from .models_rentabilidad import SucursalRentabilidad
```

### `sucursales/urls.py`
Agregar los siguientes paths al `urlpatterns` existente:
```python
from . import views_rentabilidad

urlpatterns += [
    path('rentabilidad/',                 views_rentabilidad.dashboard_rentabilidad, name='rentabilidad_dashboard'),
    path('rentabilidad/<int:pk>/',        views_rentabilidad.detalle_sucursal,       name='rentabilidad_detalle'),
    path('rentabilidad/<int:pk>/analizar/', views_rentabilidad.analizar_con_ia,      name='rentabilidad_analizar'),
    path('rentabilidad/analizar-todas/', views_rentabilidad.analizar_todas,          name='rentabilidad_analizar_todas'),
]
```

### `sucursales/tasks.py` (o `core/tasks.py` — donde estén las tareas Celery)
Agregar al final del archivo:
```python
from sucursales.tasks_rentabilidad import (
    recalcular_rentabilidad_mensual,
    recalcular_rentabilidad_periodo_actual,
    analizar_sucursal_con_ia,
)
```

### `config/settings.py` (o donde esté `CELERY_BEAT_SCHEDULE`)
Agregar estas entradas al dict `CELERY_BEAT_SCHEDULE`:
```python
"recalcular_rentabilidad_mensual": {
    "task": "sucursales.tasks_rentabilidad.recalcular_rentabilidad_mensual",
    "schedule": crontab(minute=0, hour=6, day_of_month=1),
},
"recalcular_rentabilidad_diario": {
    "task": "sucursales.tasks_rentabilidad.recalcular_rentabilidad_periodo_actual",
    "schedule": crontab(minute=30, hour=23),
},
```
Verificar que `from celery.schedules import crontab` esté importado en ese archivo.

### `config/settings.py` — Variables opcionales
Agregar en el bloque de configuración de negocio (o al final):
```python
# Umbrales de rentabilidad para el agente IA
RENT_MARGEN_BRUTO_MIN  = 55.0   # % margen bruto mínimo aceptable
RENT_MARGEN_NETO_MIN   = 15.0   # % margen neto mínimo aceptable
RENT_ROI_OBJETIVO      = 25.0   # % ROI anual objetivo
RENT_PAYBACK_MAX_MESES = 36     # meses máximo para recuperar inversión
```

---

## Migración de base de datos
Después de crear los archivos, ejecutar:
```bash
python manage.py makemigrations sucursales --name="add_sucursal_rentabilidad"
python manage.py migrate
```

---

## Verificación post-instalación
Ejecutar estos checks y reportar cualquier error:
```bash
python manage.py check
python manage.py shell -c "from sucursales.models_rentabilidad import SucursalRentabilidad; print('OK')"
```

---

## LO QUE NO DEBES HACER
- No modificar modelos existentes de `sucursales`
- No cambiar migraciones anteriores
- No tocar `api/views.py` ni `recetas/views.py`
- No instalar dependencias nuevas (todo usa lo que ya está en el proyecto)
- No cambiar URLs existentes, solo agregar

---

## Dependencia a verificar
El agente usa `openai`. Verificar que esté en `requirements.txt`. Si no está:
```bash
pip install openai
echo "openai>=1.0.0" >> requirements.txt
```
`python-dateutil` también debe estar (para `relativedelta`). Verificar igual.

---

## PASO FINAL — Integración con datos reales
Una vez instalado, en `sucursales/tasks.py` dentro de la función `recalcular_rentabilidad_mensual`,
hay un bloque comentado que dice `# Aquí integras con los apps reales del ERP`.
**No descomentar ni modificar ese bloque en esta tarea.**
Será una tarea separada cuando confirmemos los nombres exactos de los modelos de Ventas y Nómina.

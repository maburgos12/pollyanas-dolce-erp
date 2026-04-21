"""
Script de carga de gastos operativos 2026 al ERP.
Ejecutar desde la raíz del repo:
    ./.venv/bin/python importar_gastos.py

Crea CentroCosto, CategoriaGasto y GastoOperativoMensual
para todas las sucursales con datos reales del Excel.
"""

import os, sys, django, json
from pathlib import Path

# Setup Django
sys.path.insert(0, str(Path(__file__).parent))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from django.db import transaction
from core.models import Sucursal
from reportes.models import CentroCosto, CategoriaGasto, GastoOperativoMensual
from datetime import date
from decimal import Decimal

# ---- Cargar datos extraídos del Excel ----
DATA_FILE = Path(__file__).parent / "gastos_2026.json"
with open(DATA_FILE, encoding="utf-8") as f:
    registros = json.load(f)

print(f"Registros a importar: {len(registros)}")

# ---- Mapeo de categoría → tipo de gasto para clasificación ----
CATEGORIA_TIPO = {
    "NOMINA":       "NOMINA",
    "RENTA":        "RENTA",
    "SERVICIOS":    "SERVICIOS",
    "MANTENIMIENTO":"MANTENIMIENTO",
    "HIGIENE":      "HIGIENE",
    "EMPAQUE":      "EMPAQUE",
    "MARKETING":    "MARKETING",
    "INVERSION":    "INVERSION",
    "ADMIN":        "ADMIN",
    "OTROS":        "OTROS",
}

with transaction.atomic():

    # 1. Crear/obtener CentroCosto para cada sucursal
    centros = {}
    for suc in Sucursal.objects.filter(activa=True):
        centro, _ = CentroCosto.objects.get_or_create(
            codigo=suc.codigo,
            defaults={
                "nombre":   f"Sucursal {suc.nombre}",
                "sucursal": suc,
                "tipo":     "SUCURSAL_VENTA",
            }
        )
        centros[suc.codigo] = centro
        print(f"  CentroCosto: {centro.codigo} — {centro.nombre}")

    # 2. Crear/obtener CategoriaGasto para cada categoría única
    categorias = {}
    cats_unicas = set(r["categoria"] for r in registros)
    for cat in sorted(cats_unicas):
        obj, _ = CategoriaGasto.objects.get_or_create(
            codigo=cat,
            defaults={
                "nombre": cat,
                "capa_objetivo": "SUCURSAL",
                "bucket": "COMERCIAL_SUCURSAL",
                "impacta_contribucion_sucursal": True,
                "impacta_utilidad_empresa": True,
            }
        )
        categorias[cat] = obj
        print(f"  CategoriaGasto: {cat}")

    # 3. Insertar GastoOperativoMensual
    creados = 0
    actualizados = 0

    for r in registros:
        suc_codigo = r["sucursal_codigo"]
        if suc_codigo not in centros:
            print(f"  SKIP: sucursal {suc_codigo} no encontrada en ERP")
            continue

        periodo = date(r["year"], r["mes"], 1)

        obj, created = GastoOperativoMensual.objects.update_or_create(
            centro_costo=centros[suc_codigo],
            categoria_gasto=categorias[r["categoria"]],
            periodo=periodo,
            tipo_dato="REAL",
            external_key=f"{suc_codigo}-{r['year']}-{r['mes']:02d}-{r['descripcion'][:40]}",
            defaults={
                "monto":      Decimal(str(r["monto"])),
                "fuente":     "IMPORTADA",
                "comentario": r["descripcion"],
            }
        )
        if created:
            creados += 1
        else:
            actualizados += 1

    print()
    print(f"✓ Creados:     {creados}")
    print(f"✓ Actualizados:{actualizados}")
    print(f"✓ Total:       {creados + actualizados}")

    # 4. Verificación rápida
    print()
    print("=== VERIFICACIÓN ===")
    for suc in Sucursal.objects.filter(activa=True).order_by("nombre"):
        if suc.codigo not in centros:
            continue
        total = GastoOperativoMensual.objects.filter(
            centro_costo=centros[suc.codigo],
            tipo_dato="REAL"
        ).aggregate(t=__import__("django.db.models", fromlist=["Sum"]).Sum("monto"))["t"] or 0
        print(f"  {suc.nombre}: ${total:,.2f}")

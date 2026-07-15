"""Normaliza los conceptos de gasto del presupuesto a un solo formato.

Regla de dirección: una sola fuente de verdad y nomenclatura unificada —
Primera letra mayúscula y el resto minúsculas, acrónimos en mayúsculas
(IMSS, ISR, PTU), marcas con su grafía (Peugeot, Bancomer), ortografía
correcta (acentos y typos del Excel corregidos: "Mantanimiento",
"seguiridad", "PEGEOT", "Dias" → "Días").

Aplica a todas las áreas EXCEPTO Ventas (esa adopta los nombres del catálogo
Point con `renombrar_rubros_ventas_point`). El nombre anterior queda en
metadata["nombre_excel"] y el re-import del Excel lo reconoce — el nombre
viejo no vuelve. Idempotente; --dry-run para previsualizar.
"""

from __future__ import annotations

import re
import unicodedata

from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.models import RubroPresupuesto

# Siempre en mayúsculas completas.
ACRONIMOS = {"imss", "isr", "ptu", "rcv", "coepris", "capex", "sat", "iva", "jumapag", "stm"}

# Grafía propia (marcas, nombres).
GRAFIA = {
    "contpaq": "CONTPAQ",
    "point": "Point",
    "bancomer": "Bancomer",
    "edenred": "Edenred",
    "bajio": "Bajío",
    "telcel": "Telcel",
    "peugeot": "Peugeot",
    "fiat": "Fiat",
    "ducato": "Ducato",
    "cheyenne": "Cheyenne",
    "manager": "Manager",
    "partner": "Partner",
    "financial": "Financial",
    "guamuchil": "Guamúchil",
    "leyva": "Leyva",
    "colosio": "Colosio",
    "crucero": "Crucero",
    "itzel": "Itzel",
    "edmondo": "Edmondo",
    "polyana": "Polyana",
}

# Typos conocidos del Excel (en minúsculas, sin acentos).
TYPOS = {
    "mantanimiento": "mantenimiento",
    "seguiridad": "seguridad",
    "suscriciones": "suscripciones",
    "pegeot": "peugeot",
    "partnet": "partner",
    "regriferacion": "refrigeracion",
    "refigerador": "refrigerador",
    "microondras": "microondas",
    "ducate": "ducato",
}

# Acentuación correcta de palabras comunes del catálogo.
ACENTOS = {
    "dia": "día",
    "dias": "días",
    "telefono": "teléfono",
    "credito": "crédito",
    "menus": "menús",
    "articulos": "artículos",
    "adquisicion": "adquisición",
    "decoracion": "decoración",
    "fumigacion": "fumigación",
    "sanitizacion": "sanitización",
    "refrigeracion": "refrigeración",
    "produccion": "producción",
    "logistica": "logística",
    "administracion": "administración",
    "electrica": "eléctrica",
    "electrico": "eléctrico",
    "publicos": "públicos",
    "papeleria": "papelería",
    "tunel": "túnel",
    "platano": "plátano",
    "limon": "limón",
    "capacitacion": "capacitación",
    "energia": "energía",
    "arandano": "arándano",
    "computo": "cómputo",
    "nomina": "nómina",
}


def normalizar_concepto(texto: str) -> str:
    """Aplica typos → acentos → grafía/acrónimos → Primera mayúscula."""
    palabras = str(texto or "").strip().split()
    resultado: list[str] = []
    for palabra in palabras:
        # separa puntuación pegada (paréntesis, comas, diagonales se conservan)
        partes = re.split(r"([/()·,.:;-])", palabra)
        piezas: list[str] = []
        for parte in partes:
            base = parte.lower()
            # La clave se compara SIN acentos: "Guamúchil" debe encontrar su
            # grafía aunque el original ya venga acentuado.
            clave = "".join(
                c for c in unicodedata.normalize("NFKD", base) if not unicodedata.combining(c)
            )
            clave = TYPOS.get(clave, clave)
            if clave in ACRONIMOS:
                piezas.append(clave.upper())
            elif clave in GRAFIA:
                piezas.append(GRAFIA[clave])
            elif clave in ACENTOS:
                piezas.append(ACENTOS[clave])
            else:
                # Sin regla: conserva la palabra (con sus acentos), en minúscula.
                piezas.append(TYPOS.get(base, base))
        resultado.append("".join(piezas))
    frase = " ".join(resultado)
    # Primera letra alfabética en mayúscula (sin tocar acrónimos/grafías).
    for i, char in enumerate(frase):
        if char.isalpha():
            if char.islower():
                frase = frase[:i] + char.upper() + frase[i + 1 :]
            break
    return frase


class Command(BaseCommand):
    help = "Unifica la nomenclatura de los conceptos de gasto (excepto Ventas)."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        renombrados = 0
        colisiones = 0

        rubros = (
            RubroPresupuesto.objects.filter(activo=True)
            .exclude(area__codigo="ventas")
            .select_related("area", "sucursal")
            .order_by("area__orden", "concepto")
        )
        with transaction.atomic():
            for rubro in rubros:
                nuevo = normalizar_concepto(rubro.concepto)
                if nuevo == rubro.concepto:
                    continue
                colision = (
                    RubroPresupuesto.objects.filter(
                        area=rubro.area,
                        concepto=nuevo,
                        codigo_cuenta=rubro.codigo_cuenta,
                        sucursal=rubro.sucursal,
                    )
                    .exclude(pk=rubro.pk)
                    .exists()
                )
                if colision:
                    colisiones += 1
                    self.stdout.write(
                        self.style.WARNING(
                            f"  COLISIÓN [{rubro.area.codigo}]: '{rubro.concepto}' → '{nuevo}' ya existe"
                        )
                    )
                    continue
                self.stdout.write(f"  [{rubro.area.codigo}] {rubro.concepto} → {nuevo}")
                renombrados += 1
                if not dry_run:
                    metadata = dict(rubro.metadata or {})
                    metadata.setdefault("nombre_excel", rubro.concepto)
                    rubro.concepto = nuevo
                    rubro.metadata = metadata
                    rubro.save(update_fields=["concepto", "metadata", "actualizado_en"])
            if dry_run:
                transaction.set_rollback(True)

        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(f"[{modo}] renombrados: {renombrados}, colisiones: {colisiones}")

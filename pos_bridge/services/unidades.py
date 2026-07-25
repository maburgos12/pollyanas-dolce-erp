"""Conversión de cantidades entre la unidad de Point y la unidad base del ERP.

Point reporta cantidades en SU unidad por artículo (Desmoldante en ml,
Queso crema en kg); el ERP guarda en la unidad base del insumo (lt, g).
Copiar la cantidad cruda produce errores de 1000× en ambas direcciones
(casos reales 2026: ajuste de Desmoldante en −$2.3M; 60 insumos con
traspasos subestimados). Toda escritura de MovimientoInventario que venga
de Point debe pasar por aquí.
"""

from __future__ import annotations

from decimal import Decimal

from maestros.models import UnidadMedida
from recetas.utils.costeo_snapshot import POINT_UNIT_ALIASES, _compatible_units, _unit_factor


def cantidad_en_unidad_erp(cantidad: Decimal, unidad_point: str, insumo) -> tuple[Decimal, str]:
    """Convierte ``cantidad`` (en la unidad reportada por Point) a la unidad
    base del insumo ERP.

    Devuelve ``(cantidad_convertida, nota)``. Si la unidad Point no se
    reconoce se conserva la cantidad original (nota vacía); si es
    incompatible (masa vs volumen) se conserva y la nota empieza con
    ``"UNIDAD INCOMPATIBLE"`` para que el caller decida omitir/reportar.
    """
    raw = " ".join(str(unidad_point or "").strip().lower().split())
    codigo = POINT_UNIT_ALIASES.get(raw)
    destino = insumo.unidad_base if insumo is not None else None
    if not codigo or destino is None:
        return cantidad, ""
    origen = UnidadMedida.objects.filter(codigo__iexact=codigo).first()
    if origen is None or origen.id == destino.id:
        return cantidad, ""
    if not _compatible_units(origen, destino):
        return cantidad, f"UNIDAD INCOMPATIBLE: Point '{unidad_point}' vs ERP '{destino.codigo}'"
    factor_origen = _unit_factor(origen)
    factor_destino = _unit_factor(destino)
    if not factor_origen or not factor_destino:
        return cantidad, ""
    convertida = (Decimal(str(cantidad or 0)) * factor_origen / factor_destino).quantize(
        Decimal("0.000001")
    )
    return convertida, f"convertido {cantidad} {origen.codigo} → {convertida} {destino.codigo}"

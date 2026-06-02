from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from maestros.models import UnidadMedida
from recetas.utils.normalizacion import normalizar_nombre


@dataclass(frozen=True)
class RendimientoProtegido:
    nombre: str
    codigo_point: str
    cantidad: Decimal
    unidad_codigo: str


RENDIMIENTOS_PROTEGIDOS_CIRUELA: tuple[RendimientoProtegido, ...] = (
    RendimientoProtegido("Jugo de Ciruela", "03JUC64", Decimal("1.078000"), "lt"),
    RendimientoProtegido("Ciruela Cocida", "01CC07", Decimal("5.172000"), "kg"),
    RendimientoProtegido("Mermelada de Ciruela", "02MC08", Decimal("4.094000"), "kg"),
)

_BY_CODE = {item.codigo_point.upper(): item for item in RENDIMIENTOS_PROTEGIDOS_CIRUELA}
_BY_NAME = {normalizar_nombre(item.nombre): item for item in RENDIMIENTOS_PROTEGIDOS_CIRUELA}


def rendimiento_protegido_for_receta(receta) -> RendimientoProtegido | None:
    code = (getattr(receta, "codigo_point", "") or "").strip().upper()
    if code and code in _BY_CODE:
        return _BY_CODE[code]

    name = normalizar_nombre(getattr(receta, "nombre", "") or "")
    return _BY_NAME.get(name)


def protected_recipe_names() -> list[str]:
    return [item.nombre for item in RENDIMIENTOS_PROTEGIDOS_CIRUELA]


def protected_recipe_codes() -> list[str]:
    return [item.codigo_point for item in RENDIMIENTOS_PROTEGIDOS_CIRUELA]


def enforce_protected_preparation_yield(
    receta,
    rendimiento_cantidad,
    rendimiento_unidad,
) -> tuple[Decimal | None, UnidadMedida | None, bool]:
    protected = rendimiento_protegido_for_receta(receta)
    if protected is None:
        if rendimiento_cantidad is None:
            return None, rendimiento_unidad, False
        return Decimal(str(rendimiento_cantidad)), rendimiento_unidad, False

    unidad = UnidadMedida.objects.filter(codigo=protected.unidad_codigo).first()
    return protected.cantidad, unidad or rendimiento_unidad, True

from __future__ import annotations

from maestros.models import Insumo, InsumoAlias
from recetas.models import Receta, RecetaCodigoPointAlias, normalizar_codigo_point
from recetas.utils.normalizacion import normalizar_nombre


class PointMovementMatchingService:
    def resolve_receta(self, *, codigo_point: str = "", point_name: str = "") -> Receta | None:
        code = (codigo_point or "").strip()
        if code:
            receta = Receta.objects.filter(codigo_point__iexact=code).order_by("id").first()
            if receta is not None:
                return receta
            alias = (
                RecetaCodigoPointAlias.objects.filter(
                    codigo_point_normalizado=normalizar_codigo_point(code),
                    activo=True,
                )
                .select_related("receta")
                .first()
            )
            if alias and alias.receta_id:
                return alias.receta

        name_key = normalizar_nombre(point_name or "")
        if not name_key:
            return None

        alias = (
            RecetaCodigoPointAlias.objects.filter(activo=True, nombre_point__iexact=(point_name or "").strip())
            .select_related("receta")
            .first()
        )
        if alias and alias.receta_id:
            return alias.receta
        return Receta.objects.filter(nombre_normalizado=name_key).order_by("id").first()

    def resolve_insumo(self, *, codigo_point: str = "", point_name: str = "") -> Insumo | None:
        code = (codigo_point or "").strip()
        if code:
            insumo = Insumo.objects.filter(codigo_point__iexact=code).order_by("id").first()
            if insumo is not None:
                return insumo
            insumo = Insumo.objects.filter(codigo__iexact=code).order_by("id").first()
            if insumo is not None:
                return insumo

        name = (point_name or "").strip()
        if not name:
            return None

        insumo = Insumo.objects.filter(nombre_point__iexact=name).order_by("id").first()
        if insumo is not None:
            return insumo

        name_key = normalizar_nombre(name)
        if not name_key:
            return None

        alias = InsumoAlias.objects.filter(nombre_normalizado=name_key).select_related("insumo").order_by("id").first()
        if alias is not None:
            return alias.insumo
        return Insumo.objects.filter(nombre_normalizado=name_key).order_by("id").first()

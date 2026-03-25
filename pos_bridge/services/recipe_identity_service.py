from __future__ import annotations

from dataclasses import dataclass

from inventario.utils.almacen_import import CatalogMatcher
from maestros.models import Insumo, InsumoAlias, UnidadMedida
from maestros.utils.canonical_catalog import canonical_insumo
from recetas.models import Receta, RecetaCodigoPointAlias, normalizar_codigo_point
from recetas.utils.matching import match_insumo
from recetas.utils.normalizacion import normalizar_nombre


@dataclass(slots=True)
class ResolvedInsumo:
    insumo: Insumo | None
    score: float
    method: str


class PointRecipeIdentityService:
    def __init__(self):
        self._catalog_matcher: CatalogMatcher | None = None

    @property
    def catalog_matcher(self) -> CatalogMatcher:
        if self._catalog_matcher is None:
            self._catalog_matcher = CatalogMatcher()
        return self._catalog_matcher

    def refresh_catalog(self) -> None:
        self._catalog_matcher = CatalogMatcher()

    def resolve_unit(self, raw_unit) -> UnidadMedida | None:
        if not raw_unit:
            return None
        if isinstance(raw_unit, dict):
            raw_unit = raw_unit.get("Abreviacion") or raw_unit.get("Nombre") or ""
        key = normalizar_nombre(str(raw_unit or "")).replace(".", "")
        key = {
            "pieza": "pza",
            "pza": "pza",
            "pz": "pz",
            "u": "unidad",
            "und": "unidad",
            "unidad": "unidad",
            "kg": "kg",
            "g": "g",
            "gr": "g",
            "lt": "lt",
            "l": "lt",
            "ml": "ml",
        }.get(key, key)
        return UnidadMedida.objects.filter(codigo__iexact=key).first() or UnidadMedida.objects.filter(nombre__iexact=key).first()

    def resolve_insumo(
        self,
        *,
        point_code: str = "",
        point_name: str = "",
        extra_names: list[str] | None = None,
    ) -> ResolvedInsumo:
        point_code = (point_code or "").strip()
        point_name = (point_name or "").strip()
        extra_names = [name for name in (extra_names or []) if (name or "").strip()]

        if point_code:
            code_norm = normalizar_codigo_point(point_code)
            for insumo in Insumo.objects.exclude(codigo_point="").order_by("id"):
                if normalizar_codigo_point(insumo.codigo_point) == code_norm:
                    return ResolvedInsumo(insumo=canonical_insumo(insumo), score=100.0, method="POINT_CODE")

        candidate_names = [point_name, *extra_names]
        for candidate in candidate_names:
            name_norm = normalizar_nombre(candidate)
            if not name_norm:
                continue
            alias = InsumoAlias.objects.select_related("insumo").filter(nombre_normalizado=name_norm).first()
            if alias and alias.insumo_id:
                return ResolvedInsumo(insumo=canonical_insumo(alias.insumo), score=100.0, method="ALIAS")
            insumo = Insumo.objects.filter(nombre_normalizado=name_norm).order_by("id").first()
            if insumo is not None:
                return ResolvedInsumo(insumo=canonical_insumo(insumo), score=100.0, method="EXACT")

        for candidate in candidate_names:
            match = self.catalog_matcher.resolve(candidate)
            if match.insumo is not None:
                return ResolvedInsumo(insumo=canonical_insumo(match.insumo), score=float(match.score or 0), method=match.metodo)

        candidate = point_name or (extra_names[0] if extra_names else "")
        insumo, score, method = match_insumo(candidate)
        return ResolvedInsumo(insumo=canonical_insumo(insumo), score=float(score or 0), method=method or "NO_MATCH")

    def sync_insumo_point_identity(
        self,
        *,
        insumo: Insumo,
        point_code: str = "",
        point_name: str = "",
        alias_names: list[str] | None = None,
    ) -> int:
        changes = 0
        point_code = (point_code or "").strip().upper()
        point_name = (point_name or "").strip()
        alias_names = [name for name in (alias_names or []) if (name or "").strip()]

        update_fields: list[str] = []
        if point_code and insumo.codigo_point != point_code:
            insumo.codigo_point = point_code[:80]
            update_fields.append("codigo_point")
        if point_name and insumo.nombre_point != point_name:
            insumo.nombre_point = point_name[:250]
            update_fields.append("nombre_point")
        if update_fields:
            insumo.save(update_fields=update_fields)
            changes += 1

        candidate_aliases = []
        if point_name:
            candidate_aliases.append(point_name)
        candidate_aliases.extend(alias_names)
        seen: set[str] = set()
        created_alias = False
        for alias_name in candidate_aliases:
            alias_norm = normalizar_nombre(alias_name)
            if not alias_norm or alias_norm in seen:
                continue
            seen.add(alias_norm)
            alias, created = InsumoAlias.objects.get_or_create(
                nombre_normalizado=alias_norm,
                defaults={"nombre": alias_name[:250], "insumo": insumo},
            )
            if created:
                created_alias = True
                continue
            if alias.insumo_id != insumo.id:
                alias.insumo = insumo
                alias.nombre = alias_name[:250]
                alias.save(update_fields=["insumo", "nombre"])
                created_alias = True
        if created_alias:
            changes += 1
            self.refresh_catalog()
        return changes

    def get_or_create_internal_insumo(
        self,
        *,
        point_code: str,
        point_name: str,
        unidad_base: UnidadMedida | None = None,
        categoria: str = "",
    ) -> Insumo:
        resolved = self.resolve_insumo(point_code=point_code, point_name=point_name)
        if resolved.insumo is not None:
            self.sync_insumo_point_identity(
                insumo=resolved.insumo,
                point_code=point_code,
                point_name=point_name,
            )
            return resolved.insumo

        insumo = Insumo.objects.create(
            codigo_point=(point_code or "").strip().upper()[:80],
            nombre_point=(point_name or "").strip()[:250],
            nombre=(point_name or point_code or "Insumo Point")[:250],
            tipo_item=Insumo.TIPO_INTERNO,
            categoria=(categoria or "")[:120],
            unidad_base=unidad_base,
            activo=True,
        )
        self.sync_insumo_point_identity(
            insumo=insumo,
            point_code=point_code,
            point_name=point_name,
        )
        return canonical_insumo(insumo) or insumo

    def resolve_recipe(self, *, point_code: str = "", point_name: str = "") -> Receta | None:
        point_code = (point_code or "").strip()
        point_name = (point_name or "").strip()
        if point_code:
            code_norm = normalizar_codigo_point(point_code)
            receta = Receta.objects.filter(codigo_point__iexact=point_code).order_by("id").first()
            if receta is not None:
                return receta
            alias = (
                RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm, activo=True)
                .select_related("receta")
                .first()
            )
            if alias and alias.receta_id:
                return alias.receta
        if point_name:
            name_norm = normalizar_nombre(point_name)
            receta = Receta.objects.filter(nombre_normalizado=name_norm).order_by("id").first()
            if receta is not None:
                return receta
            alias = (
                RecetaCodigoPointAlias.objects.filter(nombre_point__iexact=point_name.strip(), activo=True)
                .select_related("receta")
                .first()
            )
            if alias and alias.receta_id:
                return alias.receta
        return None

    def sync_recipe_point_identity(self, *, receta: Receta, point_code: str = "", point_name: str = "") -> int:
        point_code = (point_code or "").strip().upper()
        point_name = (point_name or "").strip()
        changes = 0

        update_fields: list[str] = []
        if point_code and receta.codigo_point != point_code:
            receta.codigo_point = point_code[:80]
            update_fields.append("codigo_point")
        if update_fields:
            receta.save(update_fields=update_fields)
            changes += 1

        if not point_code:
            return changes
        code_norm = normalizar_codigo_point(point_code)
        alias, created = RecetaCodigoPointAlias.objects.get_or_create(
            codigo_point_normalizado=code_norm,
            defaults={
                "receta": receta,
                "codigo_point": point_code[:80],
                "nombre_point": point_name[:250],
                "activo": True,
            },
        )
        if created:
            return changes + 1
        update_fields = []
        if alias.receta_id != receta.id:
            alias.receta = receta
            update_fields.append("receta")
        if alias.codigo_point != point_code[:80]:
            alias.codigo_point = point_code[:80]
            update_fields.append("codigo_point")
        if point_name and alias.nombre_point != point_name[:250]:
            alias.nombre_point = point_name[:250]
            update_fields.append("nombre_point")
        if not alias.activo:
            alias.activo = True
            update_fields.append("activo")
        if update_fields:
            alias.save(update_fields=update_fields + ["actualizado_en"])
            changes += 1
        return changes

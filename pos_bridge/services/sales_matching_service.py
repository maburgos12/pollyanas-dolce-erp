from __future__ import annotations

import hashlib

from recetas.models import Receta, RecetaCodigoPointAlias, normalizar_codigo_point
from recetas.utils.temporalidad import inferir_temporalidad_receta
from recetas.utils.normalizacion import normalizar_nombre


NON_RECIPE_FAMILIES = {
    "wilton",
    "accesorios",
    "velas",
    "velas granmark",
    "plasticos",
    "regalos",
    "bebidas",
    "hielo",
}

NON_RECIPE_CATEGORIES = {
    "accesorios de reposteria",
    "granmark",
    "alegria",
    "plasticos",
    "regalos",
    "letreros",
    "velas sparklers",
    "industrias lec",
    "te",
    "hielo y agua mar de cortez",
    "otros postres",
}

NON_RECIPE_TOKENS = (
    "manga",
    "duya",
    "molde",
    "set ",
    "juego ",
    "cepillo",
    "batidora",
    "tarjeta",
    "pluma",
    "libreta",
    "vela",
    "bolsa",
    "caja ",
    "encendedor",
    "servicio",
    "decoracion",
    "deco ",
    "topping",
    "extra ",
    "contenedor",
    "aderezo",
    "gragea",
    "pirotecnia",
    "coca",
    "lonchera",
    "plato",
    "servilleta",
    "tenedor",
    "sticker",
    "taza",
    "solido",
    "empaque",
)


class PointSalesMatchingService:
    def __init__(self):
        self._point_name_index_built = False
        self._point_name_to_receta: dict[str, Receta] = {}

    def is_non_recipe_sale_row(self, payload: dict) -> bool:
        familia = normalizar_nombre(payload.get("family") or payload.get("Familia") or "")
        categoria = normalizar_nombre(payload.get("category") or payload.get("Categoria") or "")
        nombre = normalizar_nombre(payload.get("name") or payload.get("Nombre") or "")
        code = normalizar_nombre(payload.get("sku") or payload.get("Codigo") or "")

        if familia in NON_RECIPE_FAMILIES or categoria in NON_RECIPE_CATEGORIES:
            return True

        joined = f"{nombre} {code}".strip()
        return any(token in joined for token in NON_RECIPE_TOKENS)

    def _build_point_name_index(self) -> None:
        if self._point_name_index_built:
            return
        recetas_qs = Receta.objects.exclude(codigo_point="").exclude(codigo_point__isnull=True).only("id", "nombre", "nombre_normalizado")
        for receta in recetas_qs:
            key = receta.nombre_normalizado or normalizar_nombre(receta.nombre)
            if key and key not in self._point_name_to_receta:
                self._point_name_to_receta[key] = receta
        alias_qs = (
            RecetaCodigoPointAlias.objects.filter(activo=True)
            .exclude(nombre_point__isnull=True)
            .exclude(nombre_point="")
            .select_related("receta")
            .order_by("id")
        )
        for alias in alias_qs:
            if not alias.receta_id:
                continue
            key = normalizar_nombre(alias.nombre_point or "")
            if key and key not in self._point_name_to_receta:
                self._point_name_to_receta[key] = alias.receta
        self._point_name_index_built = True

    def resolve_receta(self, *, codigo_point: str, point_name: str) -> Receta | None:
        if codigo_point:
            receta = Receta.objects.filter(codigo_point__iexact=codigo_point).order_by("id").first()
            if receta is not None:
                return receta
            code_norm = normalizar_codigo_point(codigo_point)
            if code_norm:
                alias = (
                    RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm, activo=True)
                    .select_related("receta")
                    .first()
                )
                if alias and alias.receta_id:
                    return alias.receta

        if point_name:
            self._build_point_name_index()
            point_key = normalizar_nombre(point_name)
            receta = self._point_name_to_receta.get(point_key)
            if receta is not None:
                return receta
            return Receta.objects.filter(nombre_normalizado=point_key).order_by("id").first()

        return None

    def is_descriptive_product_name(self, *, point_name: str, family: str = "") -> bool:
        normalized_name = normalizar_nombre(point_name)
        if not normalized_name:
            return False
        compact = normalized_name.replace(" ", "")
        if compact.isdigit():
            return False
        alpha_tokens = [token for token in normalized_name.split() if any(ch.isalpha() for ch in token)]
        if len(alpha_tokens) >= 2:
            return True
        return bool((family or "").strip())

    def create_missing_product_recipe(
        self,
        *,
        codigo_point: str,
        point_name: str,
        category: str = "",
        family: str = "",
        dry_run: bool = False,
    ) -> Receta | None:
        base_name = (point_name or codigo_point or "").strip()
        if not base_name:
            return None

        receta = self.resolve_receta(codigo_point=codigo_point, point_name=point_name)
        if receta is not None:
            return receta

        code_raw = (codigo_point or "").strip()[:80]
        code_norm = normalizar_codigo_point(code_raw)
        norm_name = normalizar_nombre(base_name)
        if not norm_name:
            return None

        existing = Receta.objects.filter(nombre_normalizado=norm_name).order_by("id").first()
        if existing is not None:
            return existing

        seed = f"auto-point-sales|{norm_name}|{code_norm}"
        salt = 0
        hash_value = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        while Receta.objects.filter(hash_contenido=hash_value).exists():
            salt += 1
            hash_value = hashlib.sha256(f"{seed}|{salt}".encode("utf-8")).hexdigest()

        temporalidad, temporalidad_detalle = inferir_temporalidad_receta(base_name)
        receta = Receta(
            nombre=base_name[:250],
            codigo_point=code_raw,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia=(family or "")[:120],
            categoria=(category or "")[:120],
            temporalidad=temporalidad,
            temporalidad_detalle=temporalidad_detalle[:120],
            sheet_name="AUTO_POINT_SALES",
            hash_contenido=hash_value,
        )
        if not dry_run:
            receta.save()
            if code_norm:
                RecetaCodigoPointAlias.objects.get_or_create(
                    codigo_point_normalizado=code_norm,
                    defaults={
                        "receta": receta,
                        "codigo_point": code_raw,
                        "nombre_point": base_name[:250],
                        "activo": True,
                    },
                )
        else:
            receta.id = -(salt + 1)

        self._point_name_to_receta[norm_name] = receta
        return receta

    def sync_point_identity(self, *, receta: Receta, codigo_point: str, nombre_point: str) -> int:
        changed = 0
        code_raw = (codigo_point or "").strip()
        code_norm = normalizar_codigo_point(code_raw)
        name_raw = (nombre_point or "").strip()
        if not code_norm:
            return changed

        if not receta.codigo_point:
            receta.codigo_point = code_raw[:80]
            receta.save(update_fields=["codigo_point"])
            changed += 1

        alias = RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm).select_related("receta").first()
        if alias is None:
            RecetaCodigoPointAlias.objects.create(
                receta=receta,
                codigo_point=code_raw[:80],
                nombre_point=name_raw[:250] if name_raw else "",
                activo=True,
            )
            return changed + 1

        if alias.receta_id != receta.id:
            return changed

        update_fields: list[str] = []
        if code_raw and alias.codigo_point != code_raw[:80]:
            alias.codigo_point = code_raw[:80]
            update_fields.append("codigo_point")
        if name_raw and alias.nombre_point != name_raw[:250]:
            alias.nombre_point = name_raw[:250]
            update_fields.append("nombre_point")
        if not alias.activo:
            alias.activo = True
            update_fields.append("activo")
        if update_fields:
            alias.save(update_fields=update_fields + ["actualizado_en"])
            changed += 1
        return changed

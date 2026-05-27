from __future__ import annotations

import re

from django.http import QueryDict

from recetas.utils.normalizacion import normalizar_nombre

from .models import BonoEsquema, Empleado


BASE_BONO_ESQUEMAS = {
    "VENTAS": {
        "nombre": "Ventas",
        "departamento": Empleado.DEP_VENTAS,
        "area": "VENTAS",
        "descripcion": "Esquema base usado por el módulo de bonos de ventas.",
    },
    "PRODUCCION": {
        "nombre": "Producción",
        "departamento": Empleado.DEP_PRODUCCION,
        "area": "PRODUCCION",
        "descripcion": "Esquema base usado por el módulo de bonos de producción.",
    },
}


def esquema_codigo(nombre: str) -> str:
    base = normalizar_nombre(nombre or "").upper()
    return re.sub(r"[^A-Z0-9]+", "_", base).strip("_")[:60] or "BONO"


def obtener_o_crear_esquema_base(codigo: str) -> BonoEsquema:
    codigo = (codigo or "").strip().upper()
    defaults = BASE_BONO_ESQUEMAS[codigo]
    esquema, _ = BonoEsquema.objects.get_or_create(codigo=codigo, defaults=defaults)
    return esquema


def asegurar_esquemas_base() -> None:
    for codigo in BASE_BONO_ESQUEMAS:
        obtener_o_crear_esquema_base(codigo)


def _post_list(post_data: QueryDict, key: str) -> list[str]:
    if hasattr(post_data, "getlist"):
        return [str(value).strip() for value in post_data.getlist(key) if str(value).strip()]
    raw = post_data.get(key)
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(value).strip() for value in raw if str(value).strip()]
    return [str(raw).strip()] if str(raw).strip() else []


def crear_esquema_otro_desde_post(post_data) -> BonoEsquema | None:
    nombre = (post_data.get("bono_esquema_otro_nombre") or "").strip()
    if not nombre:
        return None
    codigo = esquema_codigo(nombre)
    esquema, _ = BonoEsquema.objects.update_or_create(
        codigo=codigo,
        defaults={
            "nombre": nombre,
            "departamento": (post_data.get("bono_esquema_otro_departamento") or "").strip(),
            "area": (post_data.get("bono_esquema_otro_area") or "").strip(),
            "descripcion": (post_data.get("bono_esquema_otro_descripcion") or "").strip(),
            "activo": True,
        },
    )
    return esquema


def sincronizar_esquemas_bono(empleado: Empleado, post_data, organizacion: dict) -> None:
    ids = [int(value) for value in _post_list(post_data, "bono_esquemas") if value.isdigit()]
    esquemas = list(BonoEsquema.objects.filter(pk__in=ids, activo=True))

    if post_data.get("participa_bonos_ventas") == "on" or organizacion.get("participa_bonos_ventas"):
        esquemas.append(obtener_o_crear_esquema_base("VENTAS"))
    if post_data.get("participa_bonos_produccion") == "on" or organizacion.get("participa_bonos_produccion"):
        esquemas.append(obtener_o_crear_esquema_base("PRODUCCION"))

    esquema_otro = crear_esquema_otro_desde_post(post_data)
    if esquema_otro:
        esquemas.append(esquema_otro)

    dedup = {esquema.pk: esquema for esquema in esquemas}
    empleado.bonos_esquemas.set(dedup.values())

    codigos = {esquema.codigo for esquema in dedup.values()}
    empleado.participa_bonos_ventas = "VENTAS" in codigos
    empleado.participa_bonos_produccion = "PRODUCCION" in codigos
    empleado.save(update_fields=["participa_bonos_ventas", "participa_bonos_produccion", "updated_at"])

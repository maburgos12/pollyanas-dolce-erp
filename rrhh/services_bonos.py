from __future__ import annotations

import re

from django.http import QueryDict
from django.utils import timezone

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
    sincronizar_bonos_operativos_periodo_actual(empleado)


def _bono_produccion_tiene_captura(bono) -> bool:
    if bono.estatus != "BORRADOR" or bono.registros.exists():
        return True
    campos = (
        "dias_trabajados",
        "dias_uniforme",
        "dias_puntualidad",
        "dias_asistencia",
        "dias_produccion",
        "total_embetunados",
        "monto_uniforme",
        "monto_puntualidad",
        "monto_asistencia",
        "monto_produccion",
        "monto_premio_embetunado",
        "ajuste_positivo",
        "ajuste_negativo",
        "bono_extra",
        "total_a_pagar",
    )
    return any(getattr(bono, campo) for campo in campos)


def _bono_ventas_tiene_captura(bono) -> bool:
    if bono.estatus != "BORRADOR" or bono.registros.exists():
        return True
    campos = (
        "dias_trabajados",
        "dias_asistencia",
        "dias_uniforme",
        "dias_puntualidad",
        "monto_uniforme",
        "monto_asistencia",
        "monto_puntualidad",
        "sub1",
        "bono_ventas",
        "ajuste_positivo",
        "ajuste_negativo",
        "bono_extra",
        "total_a_pagar",
    )
    return any(getattr(bono, campo) for campo in campos)


def sincronizar_bonos_operativos_periodo_actual(empleado: Empleado) -> None:
    """
    Mantiene alineado el catálogo RRHH con las tablas mensuales de bonos.
    Solo toca el periodo actual y solo elimina filas vacías en borrador.
    """
    hoy = timezone.localdate()

    from bonos_produccion.models import (
        AREA_PRODUCCION,
        AREAS_PRODUCCION,
        BonoProduccionEmpleado,
        ConfigBonoPeriodo,
        area_bono_produccion_empleado,
    )
    from bonos_ventas.models import BonoVentasEmpleado, ConfigBonoVentasPeriodo
    from core.models import Sucursal

    periodo_produccion = ConfigBonoPeriodo.objects.filter(mes=hoy.month, anio=hoy.year).first()
    if periodo_produccion:
        bonos = BonoProduccionEmpleado.objects.filter(periodo=periodo_produccion, empleado=empleado)
        if empleado.activo and empleado.participa_bonos_produccion:
            areas_validas = {codigo for codigo, _ in AREAS_PRODUCCION}
            area = area_bono_produccion_empleado(empleado)
            if area not in areas_validas:
                area = AREA_PRODUCCION
            bono, created = BonoProduccionEmpleado.objects.get_or_create(
                periodo=periodo_produccion,
                empleado=empleado,
                defaults={"area": area},
            )
            if not created and bono.area != area and not _bono_produccion_tiene_captura(bono):
                bono.area = area
                bono.save(update_fields=["area", "actualizado_en"])
        else:
            for bono in bonos:
                if not _bono_produccion_tiene_captura(bono):
                    bono.delete()

    periodo_ventas = ConfigBonoVentasPeriodo.objects.filter(mes=hoy.month, anio=hoy.year).first()
    if periodo_ventas:
        bonos = BonoVentasEmpleado.objects.filter(periodo=periodo_ventas, empleado=empleado)
        if empleado.activo and empleado.participa_bonos_ventas:
            sucursal_nombre = (empleado.sucursal or "").strip()
            sucursal = Sucursal.objects.filter(nombre__iexact=sucursal_nombre, activa=True).first()
            if sucursal:
                bono, created = BonoVentasEmpleado.objects.get_or_create(
                    periodo=periodo_ventas,
                    empleado=empleado,
                    defaults={"sucursal": sucursal},
                )
                if not created and bono.sucursal_id != sucursal.id and not _bono_ventas_tiene_captura(bono):
                    bono.sucursal = sucursal
                    bono.save(update_fields=["sucursal", "actualizado_en"])
        else:
            for bono in bonos:
                if not _bono_ventas_tiene_captura(bono):
                    bono.delete()

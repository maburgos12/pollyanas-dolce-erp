"""Alcance y contenido del pasaporte digital de activos.

Una sola puerta de lectura para el QR: quién alcanza qué activo y qué se le
entrega. La ficha se abre desde una etiqueta física, así que el alcance no
puede depender del template ni del JavaScript — quien manipule el UUID o el
POST debe chocar contra el mismo queryset que la pantalla.

Los costos se rigen exclusivamente por `mantenimiento.services_access.can_view_costs`.
No hay una regla paralela aquí: si el usuario no la pasa, las claves de costo,
factura y proveedor de servicio ni siquiera existen en el diccionario, de modo
que ningún template ni JSON pueda filtrarlas por descuido.
"""

from __future__ import annotations

from decimal import Decimal

import segno
from django.db.models import DecimalField, F, Sum, Value
from django.db.models.functions import Coalesce
from django.urls import reverse

from fallas.models import ReporteFalla
from mantenimiento.services_access import (
    authorized_branch_ids,
    can_access_mantenimiento,
    can_view_costs,
)

from .models import Activo, OrdenMantenimiento, PlanMantenimiento

# ponytail: diez eventos llenan la ficha móvil sin paginar; el historial
#           completo ya vive en Mantenimiento.
MAX_EVENTOS = 10

FALLA_ESTATUS_ABIERTOS = (
    ReporteFalla.ESTATUS_ABIERTO,
    ReporteFalla.ESTATUS_REVISION,
    ReporteFalla.ESTATUS_PROCESO,
)


def _sucursal_operativa(user):
    """Sucursal desde la que el usuario captura hoy, o `None`."""
    profile = getattr(user, "userprofile", None)
    sucursal = getattr(profile, "sucursal", None)
    if not sucursal or not sucursal.esta_operativa():
        return None
    return sucursal


def activos_autorizados(user):
    """Activos que `user` puede abrir por QR.

    Gestión de mantenimiento alcanza también activos inactivos o trasladados
    —una etiqueta impresa sobrevive a la baja del equipo—; la operación de
    sucursal sólo ve los activos vigentes de su propia sucursal, y cualquier
    otro UUID le resulta inexistente.
    """
    if not user or not user.is_authenticated:
        return Activo.objects.none()

    if can_access_mantenimiento(user):
        branch_ids = authorized_branch_ids(user)
        if branch_ids is None:
            return Activo.objects.all()
        if branch_ids:
            return Activo.objects.filter(sucursal_id__in=branch_ids)
        return Activo.objects.none()

    sucursal = _sucursal_operativa(user)
    if not sucursal:
        return Activo.objects.none()
    return Activo.objects.filter(sucursal=sucursal, activo=True)


def puede_reportar_activo(user, activo) -> bool:
    """Sólo se reporta sobre un equipo vigente de la sucursal donde se captura.

    Refleja exactamente la regla que ya aplica `operacion.views.fallas_crear_api`:
    consultar la ficha y poder abrir un reporte son permisos distintos.
    """
    if not user or not user.is_authenticated or not activo:
        return False
    if not activo.activo or not activo.sucursal_id:
        return False
    sucursal = _sucursal_operativa(user)
    return bool(sucursal and sucursal.pk == activo.sucursal_id)


def _falla_publica(falla: ReporteFalla) -> dict:
    return {
        "id": falla.pk,
        "titulo": falla.titulo,
        "prioridad": falla.prioridad,
        "prioridad_label": falla.get_prioridad_display(),
        "estatus": falla.estatus,
        "estatus_label": falla.get_estatus_display(),
        "fecha_reporte": falla.fecha_reporte,
    }


def _orden_publica(orden: OrdenMantenimiento, *, con_costos: bool) -> dict:
    fila = {
        "folio": orden.folio,
        "tipo": orden.get_tipo_display(),
        "estatus": orden.estatus,
        "estatus_label": orden.get_estatus_display(),
        "fecha": orden.fecha_cierre or orden.fecha_programada,
        "descripcion": orden.descripcion,
        "responsable": orden.responsable,
    }
    if con_costos:
        fila["costo_total"] = orden.costo_total
        fila["numero_factura"] = orden.numero_factura
        fila["proveedor_servicio"] = (
            orden.proveedor_servicio.nombre if orden.proveedor_servicio_id else ""
        )
    return fila


def _costos_activo(activo: Activo) -> dict:
    """Agrega el gasto de mantenimiento en PostgreSQL, no en Python."""
    dinero = DecimalField(max_digits=18, decimal_places=2)
    cero = Value(Decimal("0"), output_field=dinero)
    agregado = OrdenMantenimiento.objects.filter(activo_ref=activo).aggregate(
        total=Coalesce(
            Sum(
                Coalesce(F("costo_repuestos"), cero)
                + Coalesce(F("costo_mano_obra"), cero)
                + Coalesce(F("costo_otros"), cero),
                output_field=dinero,
            ),
            cero,
        )
    )
    return {
        "adquisicion": activo.costo_adquisicion,
        "mantenimiento_total": agregado["total"],
        "valor_reposicion": activo.valor_reposicion,
    }


def construir_pasaporte(activo: Activo, user) -> dict:
    """Contenido de la ficha para `user`, ya filtrado por permiso de costos."""
    con_costos = can_view_costs(user)

    fallas_abiertas = list(
        ReporteFalla.objects.filter(
            activo_relacionado=activo,
            estatus__in=FALLA_ESTATUS_ABIERTOS,
            duplicado_de__isnull=True,
        )
        .select_related("categoria", "reportado_por")
        .order_by("-fecha_reporte", "-id")[:MAX_EVENTOS]
    )
    ordenes = list(
        OrdenMantenimiento.objects.filter(activo_ref=activo)
        .select_related("proveedor_servicio")
        .order_by("-fecha_programada", "-id")[:MAX_EVENTOS]
    )
    ultimo_cierre = (
        OrdenMantenimiento.objects.filter(
            activo_ref=activo, estatus=OrdenMantenimiento.ESTATUS_CERRADA, fecha_cierre__isnull=False
        )
        .order_by("-fecha_cierre", "-id")
        .first()
    )
    plan = (
        PlanMantenimiento.objects.filter(
            activo_ref=activo,
            activo=True,
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            proxima_ejecucion__isnull=False,
        )
        .order_by("proxima_ejecucion", "id")
        .first()
    )

    pasaporte = {
        "activo": activo,
        "identidad": {
            "codigo": activo.codigo,
            "nombre": activo.nombre,
            "categoria": activo.categoria,
            "sucursal": activo.sucursal.nombre if activo.sucursal_id else "",
            "ubicacion": activo.ubicacion,
            "marca": activo.marca,
            "modelo": activo.modelo,
            "numero_serie": activo.numero_serie,
            "estado": activo.estado,
            "estado_label": activo.get_estado_display(),
            "criticidad_label": activo.get_criticidad_display(),
            "vigente": activo.activo,
        },
        "fallas_abiertas": [_falla_publica(falla) for falla in fallas_abiertas],
        "ordenes_recientes": [_orden_publica(orden, con_costos=con_costos) for orden in ordenes],
        "ultimo_mantenimiento": (
            _orden_publica(ultimo_cierre, con_costos=con_costos) if ultimo_cierre else None
        ),
        "proximo_plan": (
            {
                "nombre": plan.nombre,
                "tipo": plan.get_tipo_display(),
                "proxima_ejecucion": plan.proxima_ejecucion,
                "responsable": plan.responsable,
            }
            if plan
            else None
        ),
        "puede_ver_costos": con_costos,
        "puede_reportar": puede_reportar_activo(user, activo),
    }

    if con_costos:
        pasaporte["costos"] = _costos_activo(activo)
        pasaporte["facturas"] = [
            {
                "folio_orden": orden.folio,
                "numero": orden.numero_factura,
                "archivo": orden.factura_archivo.url if orden.factura_archivo else "",
                "fecha": orden.fecha_cierre or orden.fecha_programada,
            }
            for orden in ordenes
            if orden.numero_factura or orden.factura_archivo
        ]
        pasaporte["garantia_hasta"] = activo.garantia_hasta
        pasaporte["proveedor_compra"] = (
            activo.proveedor_compra.nombre if activo.proveedor_compra_id else ""
        )

    return pasaporte


def url_qr_activo(request, activo: Activo) -> str:
    """URL absoluta que se imprime dentro del QR: sólo esquema, host y UUID."""
    return request.build_absolute_uri(
        reverse("operacion:activo_pasaporte", args=[activo.qr_token])
    )


def svg_qr_activo(request, activo: Activo) -> str:
    """SVG inline del QR.

    Corrección de error H y zona silenciosa de cuatro módulos: la etiqueta vive
    pegada a un horno o una vitrina y se lee sucia, rayada y de lejos. El
    contenido es sólo la URL construida arriba, nunca datos del activo, y por
    eso el SVG puede marcarse como seguro en el template.
    """
    url = url_qr_activo(request, activo)
    return segno.make_qr(url, error="h").svg_inline(scale=4, border=4)

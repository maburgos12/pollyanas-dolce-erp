from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import transaction

from activos.models import Activo
from core.models import Sucursal
from logistica.models import Unidad
from maestros.models import Proveedor
from mantenimiento.models import DetalleServicioMantenimiento, ServicioMantenimiento


def _money(value, *, allow_none=False):
    if value in (None, "") and allow_none:
        return None
    try:
        amount = Decimal(str(value or "0")).quantize(Decimal("0.01"))
    except Exception as exc:
        raise ValidationError("Captura un importe válido.") from exc
    if amount < 0:
        raise ValidationError("Los importes no pueden ser negativos.")
    return amount


def _resolve_provider(name, provider_id=None):
    clean_name = (name or "").strip()
    if provider_id:
        provider = Proveedor.objects.filter(pk=provider_id, activo=True).first()
        if provider is None:
            raise ValidationError("El proveedor seleccionado no existe o está inactivo.")
        return provider
    if not clean_name:
        return None
    provider = Proveedor.objects.filter(nombre__iexact=clean_name).first()
    if provider:
        return provider
    return Proveedor.objects.create(nombre=clean_name)


def _build_detail(raw, *, allowed_branch_ids=None):
    kind = str(raw.get("tipo_objetivo") or "").upper().strip()
    detail = DetalleServicioMantenimiento(
        tipo_objetivo=kind,
        trabajo_realizado=str(raw.get("trabajo_realizado") or "").strip(),
        ubicacion=str(raw.get("ubicacion") or "").strip(),
        instalacion_categoria=str(raw.get("instalacion_categoria") or "").strip(),
        costo_asignado=_money(raw.get("costo_asignado"), allow_none=True),
        costo_estimado=bool(raw.get("costo_estimado")),
        proxima_revision=raw.get("proxima_revision") or None,
    )
    if not detail.trabajo_realizado:
        raise ValidationError("Describe el trabajo realizado en cada equipo o instalación.")

    if kind == detail.OBJETIVO_ACTIVO:
        detail.activo = Activo.objects.select_related("sucursal").filter(
            pk=raw.get("activo_id"), activo=True,
        ).first()
        branch_id = detail.activo.sucursal_id if detail.activo else None
    elif kind == detail.OBJETIVO_UNIDAD:
        detail.unidad = Unidad.objects.select_related("sucursal").filter(
            pk=raw.get("unidad_id"), activa=True,
        ).first()
        branch_id = detail.unidad.sucursal_id if detail.unidad else None
    elif kind == detail.OBJETIVO_INSTALACION:
        detail.sucursal = Sucursal.objects.filter(pk=raw.get("sucursal_id"), activa=True).first()
        branch_id = detail.sucursal_id
    else:
        branch_id = None

    if allowed_branch_ids is not None and branch_id not in allowed_branch_ids:
        raise ValidationError("Uno de los alcances no pertenece a una sucursal autorizada.")
    detail.full_clean(exclude=["servicio"])
    return detail


@transaction.atomic
def create_grouped_service(*, payload, details, user, allowed_branch_ids=None):
    """Create one economic document and N technical history lines atomically."""
    if not details:
        raise ValidationError("Agrega por lo menos un equipo o trabajo en instalaciones.")

    source_key = (payload.get("clave_origen") or "").strip() or None
    if source_key:
        existing = ServicioMantenimiento.objects.filter(clave_origen=source_key).first()
        if existing:
            return existing, False

    built_details = [_build_detail(raw, allowed_branch_ids=allowed_branch_ids) for raw in details]
    branch_ids = {
        branch_id for branch_id in (
            item.activo.sucursal_id if item.activo_id else
            item.unidad.sucursal_id if item.unidad_id else item.sucursal_id
            for item in built_details
        ) if branch_id
    }
    charge_branch_id = payload.get("sucursal_cargo_id") or None
    if charge_branch_id:
        charge_branch = Sucursal.objects.filter(pk=charge_branch_id, activa=True).first()
        if charge_branch is None:
            raise ValidationError("Selecciona un centro de costo válido.")
    elif len(branch_ids) == 1:
        charge_branch = Sucursal.objects.get(pk=next(iter(branch_ids)))
    else:
        charge_branch = None

    service = ServicioMantenimiento(
        fecha_servicio=payload.get("fecha_servicio"),
        proveedor=_resolve_provider(payload.get("proveedor_nombre"), payload.get("proveedor_id")),
        proveedor_nombre=(payload.get("proveedor_nombre") or "").strip(),
        sucursal_cargo=charge_branch,
        responsable=(payload.get("responsable") or "").strip(),
        numero_documento=(payload.get("numero_documento") or "").strip(),
        documento=payload.get("documento"),
        descripcion_general=(payload.get("descripcion_general") or "").strip(),
        costo_total=_money(payload.get("costo_total")),
        metodo_distribucion=payload.get("metodo_distribucion") or ServicioMantenimiento.DISTRIBUCION_SIN_DESGLOSE,
        clave_origen=source_key,
        creado_por=user,
    )
    service.full_clean()

    assigned = sum((item.costo_asignado or Decimal("0") for item in built_details), Decimal("0"))
    if service.metodo_distribucion == ServicioMantenimiento.DISTRIBUCION_SIN_DESGLOSE:
        if assigned:
            raise ValidationError("Sin desglose: deja vacíos los costos por equipo o instalación.")
        if service.costo_total and service.sucursal_cargo_id is None:
            raise ValidationError("Selecciona el centro de costo de una factura que cubre varias sucursales.")
    elif assigned != service.costo_total:
        raise ValidationError("El costo distribuido entre los alcances debe coincidir con el total del documento.")

    service.save()
    for detail in built_details:
        detail.servicio = service
        detail.save()
    return service, True

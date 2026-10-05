"""Correspondencias documentales explícitas; nunca fusionan los catálogos."""
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction

from core.access import can_manage_submodule, can_view_submodule
from core.models import AuditLog
from maestros.models import Proveedor
from .models import ProveedorServicio, VinculoProveedorDocumental
from .services_access import can_access_mantenimiento, can_write_mantenimiento


def actor_actual(user):
    if not user or not user.is_authenticated or not user.pk:
        raise PermissionDenied("Debes iniciar sesión.")
    try:
        actor = get_user_model().objects.get(pk=user.pk)
    except get_user_model().DoesNotExist:
        raise PermissionDenied("La cuenta ya no está disponible.")
    if not actor.is_active:
        raise PermissionDenied("La cuenta está inactiva.")
    return actor


def _limite_explicito_mantenimiento(actor):
    if actor.is_superuser:
        return "manage"
    explicit = dict(actor.module_access.filter(
        module__in=["mantenimiento", "mantenimiento.app", "mantenimiento.bandeja", "mantenimiento.dashboard"]
    ).values_list("module", "access"))
    if "mantenimiento" in explicit:
        return explicit["mantenimiento"]
    children = ["mantenimiento.app", "mantenimiento.bandeja", "mantenimiento.dashboard"]
    if all(key in explicit for key in children):
        values = {explicit[key] for key in children}
        return "manage" if "manage" in values else "view" if "view" in values else "none"
    return None


def puede_leer_vinculos(actor):
    # The legacy role gate accepts groups independently of explicit overrides.
    # Complete revocations must not be revived by that fallback.
    return (_limite_explicito_mantenimiento(actor) != "none" and can_access_mantenimiento(actor)
            and can_view_submodule(actor, "maestros", "proveedores"))


def puede_crear_vinculos(actor):
    return (_limite_explicito_mantenimiento(actor) not in {"none", "view"}
            and puede_leer_vinculos(actor) and can_write_mantenimiento(actor)
            and can_manage_submodule(actor, "maestros", "proveedores"))


@transaction.atomic
def confirmar_vinculo(*, user, perfil_id, proveedor_id, motivo, evidencia, confirmado):
    actor = actor_actual(user)
    if not puede_crear_vinculos(actor):
        raise PermissionDenied("Necesitas gestionar Mantenimiento y Maestros · Proveedores.")
    motivo, evidencia = str(motivo or "").strip(), str(evidencia or "").strip()
    if confirmado is not True or not motivo or not evidencia:
        raise ValidationError("Confirma la correspondencia e indica motivo y evidencia documental.")
    try:
        perfil_id, proveedor_id = int(perfil_id), int(proveedor_id)
    except (ValueError, TypeError):
        raise ValidationError("Selecciona los dos registros existentes.")
    # Consistent order; NO KEY UPDATE avoids FK key-share deadlocks. The source
    # locks serialize retries of a pair, including submissions by other actors.
    perfil = ProveedorServicio.objects.select_for_update(no_key=True).filter(pk=perfil_id).first()
    proveedor = Proveedor.objects.select_for_update(no_key=True).filter(pk=proveedor_id).first()
    if not perfil or not proveedor:
        raise ValidationError("Uno de los registros ya no existe. No se creó ningún registro.")
    if not perfil.activo or not proveedor.activo:
        raise ValidationError("Los dos registros deben estar activos para confirmar un vínculo.")
    vinculo, creado = VinculoProveedorDocumental.objects.get_or_create(
        perfil_original_id=perfil.pk, proveedor_original_id=proveedor.pk,
        defaults={"perfil": perfil, "proveedor": proveedor, "motivo": motivo,
                  "evidencia": evidencia, "autor": actor, "autor_original_id": actor.pk},
    )
    if not creado:
        if vinculo.motivo != motivo or vinculo.evidencia != evidencia:
            raise ValidationError("El par ya tiene un vínculo confirmado. Se conserva su motivo y evidencia originales.", code="conflict")
        return vinculo, False
    AuditLog.objects.create(
        user=actor, action="CREATE", model="mantenimiento.VinculoProveedorDocumental",
        object_id=str(vinculo.pk), payload={"perfil_id": perfil.pk, "proveedor_id": proveedor.pk,
                                           "motivo": motivo, "evidencia": evidencia},
    )
    return vinculo, True

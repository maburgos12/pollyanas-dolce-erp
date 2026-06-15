from __future__ import annotations

from django.contrib.auth.models import Group
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.utils import timezone

from core.models import Sucursal, UserProfile
from recetas.utils.normalizacion import normalizar_nombre

from .models import Empleado, EmpleadoIdentidadPendiente


_UNSET = object()


def normalizar_codigo_empleado(codigo: str | None) -> str:
    return (codigo or "").strip()


def buscar_empleado_por_codigo(codigo: str | None) -> Empleado | None:
    codigo = normalizar_codigo_empleado(codigo)
    if not codigo:
        return None
    return Empleado.objects.filter(codigo__iexact=codigo).first()


def sugerir_empleado_por_nombre(nombre: str | None) -> Empleado | None:
    nombre_norm = normalizar_nombre(nombre or "")
    if not nombre_norm:
        return None

    exactos = list(Empleado.objects.filter(nombre_normalizado=nombre_norm).order_by("-activo", "id")[:2])
    if len(exactos) == 1:
        return exactos[0]
    if len(exactos) > 1:
        return None

    candidatos = list(
        Empleado.objects.filter(nombre_normalizado__icontains=nombre_norm)
        .order_by("-activo", "id")
        .only("id", "nombre", "nombre_normalizado", "activo")[:2]
    )
    if len(candidatos) == 1:
        return candidatos[0]
    return None


def empleado_vinculado_usuario(user) -> Empleado | None:
    if not user:
        return None
    try:
        return user.empleado_rrhh
    except ObjectDoesNotExist:
        return None


def nombre_operativo_usuario(user) -> str:
    """
    Nombre visible para pantallas operativas.

    La persona real vive en RRHH. Si el usuario ERP esta ligado a Empleado,
    ese nombre tiene prioridad sobre first_name/last_name.
    """
    if not user:
        return ""
    empleado = empleado_vinculado_usuario(user)
    if empleado and empleado.nombre:
        return empleado.nombre
    return user.get_full_name() or user.username


def sincronizar_nombre_usuario_desde_empleado(empleado: Empleado) -> bool:
    user = empleado.usuario_erp
    if not user or not empleado.nombre:
        return False
    if user.get_full_name():
        return False
    user.first_name = empleado.nombre.strip()
    user.last_name = ""
    user.save(update_fields=["first_name", "last_name"])
    return True


def asegurar_identidad_operativa_empleado(
    empleado: Empleado,
    *,
    sucursal_app_id: int | None | object = _UNSET,
) -> dict[str, bool]:
    """
    Proyecta la identidad RRHH hacia usuario/perfil/repartidor sin duplicar persona.

    No crea usuarios. Solo completa piezas operativas cuando Empleado.usuario_erp
    ya fue seleccionado en Capital Humano.
    """
    result = {"user_name_synced": False, "profile_synced": False, "repartidor_synced": False}
    if not empleado.usuario_erp_id:
        return result

    result["user_name_synced"] = sincronizar_nombre_usuario_desde_empleado(empleado)
    profile, _ = UserProfile.objects.get_or_create(user_id=empleado.usuario_erp_id)
    if sucursal_app_id is not _UNSET and profile.sucursal_id != sucursal_app_id:
        profile.sucursal_id = sucursal_app_id
        profile.save(update_fields=["sucursal"])
        result["profile_synced"] = True

    sucursal = (
        Sucursal.objects.filter(pk=sucursal_app_id).first()
        if isinstance(sucursal_app_id, int)
        else _sucursal_empleado(empleado)
    )
    result["repartidor_synced"] = asegurar_repartidor_logistica(empleado, sucursal=sucursal)
    return result


@transaction.atomic
def desactivar_identidad_operativa_empleado(empleado: Empleado) -> dict[str, bool]:
    result = {"empleado_deactivated": False, "user_deactivated": False, "repartidor_group_removed": False}
    if empleado.activo:
        empleado.activo = False
        empleado.save(update_fields=["activo", "updated_at"])
        result["empleado_deactivated"] = True

    user = empleado.usuario_erp
    if not user:
        return result

    if user.is_active:
        user.is_active = False
        user.save(update_fields=["is_active"])
        result["user_deactivated"] = True

    grupos_repartidor = list(user.groups.filter(name__iexact="repartidor"))
    if grupos_repartidor:
        user.groups.remove(*grupos_repartidor)
        result["repartidor_group_removed"] = True

    return result


def asegurar_repartidor_logistica(empleado: Empleado, *, sucursal: Sucursal | None = None) -> bool:
    if not empleado.usuario_erp_id or (empleado.puesto_operativo or "").strip().upper() != "REPARTIDOR":
        return False
    sucursal = sucursal or _sucursal_empleado(empleado)
    if not sucursal:
        return False

    from logistica.models import Repartidor

    repartidor, created = Repartidor.objects.get_or_create(
        user_id=empleado.usuario_erp_id,
        defaults={
            "sucursal": sucursal,
            "telefono": empleado.telefono or "",
        },
    )
    changed = created
    if repartidor.sucursal_id != sucursal.id:
        repartidor.sucursal = sucursal
        changed = True
    if empleado.telefono and not repartidor.telefono:
        repartidor.telefono = empleado.telefono
        changed = True
    if changed and not created:
        repartidor.save(update_fields=["sucursal", "telefono"])

    grupo, _ = Group.objects.get_or_create(name="repartidor")
    empleado.usuario_erp.groups.add(grupo)
    return changed


def _sucursal_empleado(empleado: Empleado) -> Sucursal | None:
    value = (empleado.sucursal or "").strip()
    if not value:
        return None
    key = normalizar_nombre(value)
    for sucursal in Sucursal.objects.filter(activa=True):
        if normalizar_nombre(sucursal.nombre) == key or normalizar_nombre(sucursal.codigo) == key:
            return sucursal
    return None


def registrar_identidad_pendiente(
    *,
    fuente: str,
    codigo_externo: str | None,
    nombre_externo: str | None,
    notas: str = "",
) -> EmpleadoIdentidadPendiente | None:
    codigo = normalizar_codigo_empleado(codigo_externo)
    nombre = (nombre_externo or "").strip()
    if not codigo:
        return None

    empleado_codigo = buscar_empleado_por_codigo(codigo)
    if empleado_codigo:
        return None

    sugerido = sugerir_empleado_por_nombre(nombre)
    pendiente, _ = EmpleadoIdentidadPendiente.objects.update_or_create(
        fuente=fuente,
        codigo_externo=codigo,
        defaults={
            "nombre_externo": nombre,
            "empleado_sugerido": sugerido,
            "estado": EmpleadoIdentidadPendiente.ESTADO_PENDIENTE,
            "notas": notas,
        },
    )
    return pendiente


@transaction.atomic
def vincular_identidad_pendiente(
    pendiente: EmpleadoIdentidadPendiente,
    empleado: Empleado,
    *,
    user=None,
) -> Empleado:
    codigo = normalizar_codigo_empleado(pendiente.codigo_externo)
    if not codigo:
        raise ValueError("La identidad pendiente no tiene código externo.")

    usado_por = Empleado.objects.filter(codigo__iexact=codigo).exclude(pk=empleado.pk).first()
    if usado_por:
        raise ValueError(f"El código {codigo} ya pertenece a {usado_por.nombre}.")

    empleado.codigo = codigo
    empleado.save(update_fields=["codigo", "nombre_normalizado", "updated_at"])

    pendiente.empleado_sugerido = empleado
    pendiente.estado = EmpleadoIdentidadPendiente.ESTADO_VINCULADO
    pendiente.resuelto_por = user
    pendiente.resuelto_en = timezone.now()
    pendiente.save(update_fields=["empleado_sugerido", "estado", "resuelto_por", "resuelto_en", "actualizado_en"])
    return empleado


def cerrar_identidad_por_codigo_existente(
    pendiente: EmpleadoIdentidadPendiente,
    *,
    user=None,
) -> Empleado:
    empleado = buscar_empleado_por_codigo(pendiente.codigo_externo)
    if not empleado:
        raise ValueError("El código todavía no está ligado a ningún empleado.")
    return vincular_identidad_pendiente(pendiente, empleado, user=user)


def descartar_identidad_pendiente(
    pendiente: EmpleadoIdentidadPendiente,
    *,
    user=None,
    notas: str = "",
) -> EmpleadoIdentidadPendiente:
    notas = (notas or "").strip()
    if notas:
        pendiente.notas = "\n\n".join(
            filter(None, [pendiente.notas.strip(), f"Descartado por RRHH: {notas}"])
        )
    pendiente.estado = EmpleadoIdentidadPendiente.ESTADO_DESCARTADO
    pendiente.resuelto_por = user
    pendiente.resuelto_en = timezone.now()
    pendiente.save(update_fields=["notas", "estado", "resuelto_por", "resuelto_en", "actualizado_en"])
    return pendiente

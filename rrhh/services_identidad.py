from __future__ import annotations

from django.db import transaction
from django.utils import timezone

from recetas.utils.normalizacion import normalizar_nombre

from .models import Empleado, EmpleadoIdentidadPendiente


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

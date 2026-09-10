"""Alta explícita y auditada de proveedores de servicio.

Usada por Fallas y por el seguimiento de Mantenimiento: ambas pantallas crean en
el mismo catálogo (mantenimiento.ProveedorServicio), sin catálogos paralelos.
"""

import unicodedata

from django.db import connection, transaction

from core.models import AuditLog

from .models import ProveedorServicio


def clave_nombre(nombre: str) -> str:
    """Nombre comparable: sin acentos, sin distinción de mayúsculas ni espacios extra."""
    sin_acentos = "".join(
        c for c in unicodedata.normalize("NFKD", nombre) if not unicodedata.combining(c)
    )
    return " ".join(sin_acentos.casefold().split())


def alta_proveedor_servicio(serializer, user, origen):
    """Crea el proveedor validado o devuelve el conflicto con el existente.

    Devuelve (payload, status) listo para Response. No modifica registros
    existentes: un duplicado responde 409 para que la pantalla lo seleccione.
    """
    with transaction.atomic():
        # Serialize concurrent registrations, including an empty catalog.
        with connection.cursor() as cursor:
            cursor.execute("LOCK TABLE mantenimiento_proveedorservicio IN SHARE ROW EXCLUSIVE MODE")
        nombre = serializer.validated_data["nombre"]
        existente = next(
            (p for p in ProveedorServicio.objects.order_by("pk")
             if clave_nombre(p.nombre) == clave_nombre(nombre)),
            None,
        )
        if existente:
            mensaje = (
                f"Ya existe «{existente.nombre}». Selecciónalo en la lista." if existente.activo
                else f"«{existente.nombre}» ya está registrado como inactivo. Revisa su ficha en Mantenimiento."
            )
            conflicto = {
                "ok": False,
                "errors": {"nombre": [mensaje]},
                "toast": {"type": "warning", "message": mensaje},
            }
            if existente.activo:
                # Permite que la pantalla seleccione el existente en vez de duplicarlo.
                conflicto["proveedor"] = serializer.__class__(existente).data
            return conflicto, 409
        proveedor = serializer.save()
        AuditLog.objects.create(
            user=user, action="CREATE", model="mantenimiento.ProveedorServicio",
            object_id=str(proveedor.pk), payload={"origen": origen, "nombre": proveedor.nombre},
        )
    return {
        "ok": True,
        "proveedor": serializer.__class__(proveedor).data,
        "toast": {"type": "success", "message": "Proveedor guardado y seleccionado."},
    }, 201

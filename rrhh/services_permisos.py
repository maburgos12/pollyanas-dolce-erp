from __future__ import annotations

from django.contrib.auth.models import AbstractBaseUser

from core.access import ROLE_DG
from recetas.utils.normalizacion import normalizar_nombre

from .models import Empleado


DIRECCION_DEPARTAMENTOS = {
    Empleado.DEP_ADMINISTRACION,
    Empleado.DEP_VENTAS,
    Empleado.DEP_PRODUCCION,
    Empleado.DEP_RRHH,
    Empleado.DEP_COMPRAS,
    Empleado.DEP_LOGISTICA,
}

DIRECCION_NOMBRES = {
    "YESENIA SOTO INZUNZA",
    "JOHANA LOPEZ",
    "JOHANA LOPEZ LOPEZ",
    "CAROLINA CAYETANO",
    "PAULA",
    "PAULA LUGO",
}

DIRECCION_PUESTO_KEYWORDS = (
    "JEFE",
    "ENCARGAD",
    "RESPONSABLE",
    "COORDINADOR",
    "LIDER",
)


def can_authorize_direccion(user: AbstractBaseUser) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return user.groups.filter(name__iexact=ROLE_DG).exists()


def permiso_requiere_autorizacion_direccion(empleado: Empleado | None) -> bool:
    """
    Solo jefaturas/reportes directos a DG pasan por autorización de Dirección.
    El personal operativo mantiene flujo jefe directo -> RRHH.
    """
    if not empleado:
        return False

    nombre = normalizar_nombre(empleado.nombre or "")
    nombres_direccion = {normalizar_nombre(item) for item in DIRECCION_NOMBRES}
    if nombre in nombres_direccion or any(nombre.startswith(item) for item in nombres_direccion if len(item) > 5):
        return True

    departamento = (empleado.departamento or empleado.departamento_origen or "").strip().upper()
    if departamento not in DIRECCION_DEPARTAMENTOS:
        return False

    puesto = f"{empleado.puesto or ''} {empleado.puesto_operativo or ''} {empleado.area or ''}".upper()
    if any(keyword in puesto for keyword in DIRECCION_PUESTO_KEYWORDS):
        return True

    return departamento in {Empleado.DEP_COMPRAS, Empleado.DEP_LOGISTICA} and not empleado.jefe_directo_id

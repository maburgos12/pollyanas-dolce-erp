"""Resolución de contactos de una persona del ERP con precedencia explícita.

Precedencia (documentada y única para todo el ERP):

* Correo: ``User.email`` → ``Empleado.email`` del empleado ligado por
  ``usuario_erp``.
* Teléfono: ``UserProfile.telefono`` → ``Empleado.telefono`` del mismo empleado.

El dato de la cuenta va primero porque es el que la persona mantiene y con el
que el ERP ya la identifica; el expediente de Capital Humano es el respaldo.
Nunca se sustituye por el contacto de otra persona: si el titular no tiene dato
válido, la resolución devuelve vacío con su motivo.
"""
from __future__ import annotations

import re

from django.core.exceptions import ValidationError
from django.core.validators import validate_email

SIN_CORREO = "Sin correo registrado"
SIN_TELEFONO = "Sin teléfono registrado"
CORREO_INVALIDO = "Correo registrado con formato inválido"
TELEFONO_INVALIDO = "Teléfono registrado con formato inválido"


def empleado_de_usuario(usuario):
    """Empleado ligado por ``usuario_erp``; nunca empareja por nombre o correo."""
    return getattr(usuario, "empleado_rrhh", None)


def normalizar_telefono(raw: str) -> str:
    """Devuelve el número en dígitos E.164 sin '+', o '' si no es utilizable.

    Un número mexicano de 10 dígitos se completa con la lada 52. Cualquier otro
    valor debe venir ya en formato internacional.
    """
    texto = (raw or "").strip()
    if not texto:
        return ""
    if texto.startswith("whatsapp:"):
        texto = texto.split(":", 1)[1]
    limpio = re.sub(r"[^0-9+]", "", texto)
    if limpio.startswith("00"):
        limpio = "+" + limpio[2:]
    digitos = re.sub(r"\D", "", limpio)
    if len(digitos) == 10:
        digitos = f"52{digitos}"
    if not 11 <= len(digitos) <= 15:
        return ""
    return digitos


def _candidatos_correo(usuario) -> list[str]:
    empleado = empleado_de_usuario(usuario)
    return [
        (getattr(usuario, "email", "") or "").strip(),
        (getattr(empleado, "email", "") or "").strip(),
    ]


def _candidatos_telefono(usuario) -> list[str]:
    empleado = empleado_de_usuario(usuario)
    perfil = getattr(usuario, "userprofile", None)
    return [
        (getattr(perfil, "telefono", "") or "").strip(),
        (getattr(empleado, "telefono", "") or "").strip(),
    ]


def resolver_correo(usuario) -> tuple[str, str]:
    """Devuelve ``(correo, motivo)``. Con correo válido el motivo queda vacío."""
    if not usuario:
        return "", SIN_CORREO
    candidatos = [valor for valor in _candidatos_correo(usuario) if valor]
    if not candidatos:
        return "", SIN_CORREO
    for valor in candidatos:
        try:
            validate_email(valor)
        except ValidationError:
            continue
        return valor, ""
    return "", CORREO_INVALIDO


def resolver_telefono(usuario) -> tuple[str, str]:
    """Devuelve ``(telefono_e164, motivo)``. Con teléfono válido el motivo queda vacío."""
    if not usuario:
        return "", SIN_TELEFONO
    candidatos = [valor for valor in _candidatos_telefono(usuario) if valor]
    if not candidatos:
        return "", SIN_TELEFONO
    for valor in candidatos:
        normalizado = normalizar_telefono(valor)
        if normalizado:
            return normalizado, ""
    return "", TELEFONO_INVALIDO


def nombre_para_saludo(usuario) -> str:
    if not usuario:
        return "colaborador"
    empleado = empleado_de_usuario(usuario)
    nombre_empleado = (getattr(empleado, "nombre", "") or "").strip()
    return (usuario.get_full_name() or "").strip() or nombre_empleado or usuario.username

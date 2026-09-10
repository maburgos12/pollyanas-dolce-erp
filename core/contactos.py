"""Contacto de trabajo de una persona del ERP.

El ERP guarda dos cosas distintas que no deben mezclarse:

* **Contacto de trabajo** — la cuenta del ERP: ``User.email`` (correo
  ``@pollyanasdolce.com``) y ``UserProfile.telefono`` (línea propiedad de
  Pollyana's Dolce, asignada a un puesto o departamento). Es lo único que se usa
  para avisos operativos.
* **Contacto personal** — el expediente de Capital Humano: ``Empleado.email`` y
  ``Empleado.telefono``. Son el gmail y el celular propios del colaborador.
  Existen para RRHH, **no** son un respaldo del contacto de trabajo.

Por eso aquí **no hay fallback al expediente**: si la persona no tiene contacto
de trabajo registrado, la resolución devuelve vacío con su motivo y el aviso lo
dice. Nunca se manda un asunto de trabajo al teléfono o correo personal, ni se
sustituye por el contacto de otra persona.
"""
from __future__ import annotations

import re

from django.core.exceptions import ValidationError
from django.core.validators import validate_email

SIN_CORREO = "Sin correo de trabajo registrado"
SIN_TELEFONO = "Sin teléfono de trabajo registrado"
CORREO_INVALIDO = "Correo de trabajo con formato inválido"
TELEFONO_INVALIDO = "Teléfono de trabajo con formato inválido"


def empleado_de_usuario(usuario):
    """Empleado ligado por ``usuario_erp``; nunca empareja por nombre o correo.

    Se usa solo para el nombre de la persona, no para resolver contactos.
    """
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


def resolver_correo(usuario) -> tuple[str, str]:
    """Correo de trabajo de la cuenta. Devuelve ``(correo, motivo)``."""
    if not usuario:
        return "", SIN_CORREO
    valor = (getattr(usuario, "email", "") or "").strip()
    if not valor:
        return "", SIN_CORREO
    try:
        validate_email(valor)
    except ValidationError:
        return "", CORREO_INVALIDO
    return valor, ""


def resolver_telefono(usuario) -> tuple[str, str]:
    """Línea de trabajo del perfil. Devuelve ``(telefono_e164, motivo)``."""
    if not usuario:
        return "", SIN_TELEFONO
    perfil = getattr(usuario, "userprofile", None)
    valor = (getattr(perfil, "telefono", "") or "").strip()
    if not valor:
        return "", SIN_TELEFONO
    normalizado = normalizar_telefono(valor)
    if not normalizado:
        return "", TELEFONO_INVALIDO
    return normalizado, ""


def nombre_para_saludo(usuario) -> str:
    if not usuario:
        return "colaborador"
    empleado = empleado_de_usuario(usuario)
    nombre_empleado = (getattr(empleado, "nombre", "") or "").strip()
    return (usuario.get_full_name() or "").strip() or nombre_empleado or usuario.username

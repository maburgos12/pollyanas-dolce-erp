from __future__ import annotations

from django.contrib.auth.models import AnonymousUser

from sat_client.models import SolicitudDocumentoSat
from sat_client.services.base import SatConfigurationError, get_sat_credentials


def solicitar_documento_sat(*, tipo: str, usuario) -> SolicitudDocumentoSat:
    if tipo not in dict(SolicitudDocumentoSat.TIPO_CHOICES):
        raise ValueError("Tipo de documento SAT no valido.")
    try:
        get_sat_credentials()
    except SatConfigurationError as exc:
        estado = SolicitudDocumentoSat.ESTADO_ERROR
        mensaje = str(exc)
    else:
        estado = SolicitudDocumentoSat.ESTADO_PENDIENTE
        mensaje = "Conector de portal SAT pendiente: este documento no se obtiene por descarga masiva CFDI."
    return SolicitudDocumentoSat.objects.create(
        tipo=tipo,
        estado=estado,
        mensaje=mensaje,
        solicitado_por=None if isinstance(usuario, AnonymousUser) else usuario,
    )

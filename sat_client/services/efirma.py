from __future__ import annotations

import base64
import json
import re
from pathlib import Path

from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone
from django.utils.crypto import salted_hmac
from django.views.decorators.debug import sensitive_variables

from sat_client.models import ConfiguracionEfirma, LogDescargaSat
from sat_client.services.autenticacion import obtener_token
from sat_client.services.base import SatConfigurationError, SatCredentials, SatServiceError
from sat_client.services.firma import load_private_key

MAX_FILE_BYTES = 16 * 1024


def _cipher() -> MultiFernet:
    # Keep previous SECRET_KEY values in SECRET_KEY_FALLBACKS until credentials are re-encrypted.
    keys = [settings.SECRET_KEY, *settings.SECRET_KEY_FALLBACKS]
    return MultiFernet([Fernet(base64.urlsafe_b64encode(salted_hmac(
        "sat_client.efirma.v1", "encryption", secret=key, algorithm="sha256"
    ).digest())) for key in keys])


@sensitive_variables()
def cargar_efirma() -> SatCredentials | None:
    saved = ConfiguracionEfirma.objects.filter(pk=1).first()
    if saved is None:
        return None
    try:
        data = json.loads(_cipher().decrypt(bytes(saved.contenido_cifrado)))
        if data["rfc"] != saved.rfc or saved.rfc != (getattr(settings, "SAT_RFC", "") or "").strip().upper():
            raise ValueError("RFC distinto")
        return SatCredentials("", "", data["password"], data["rfc"],
                              base64.b64decode(data["cer"], validate=True),
                              base64.b64decode(data["key"], validate=True))
    except (InvalidToken, ValueError, KeyError, TypeError) as exc:
        raise SatConfigurationError("No se puede leer la e.firma guardada. Revisa la clave de cifrado y el RFC configurado.") from exc


@sensitive_variables()
def validar_efirma(cer: bytes, key: bytes, password: str) -> tuple[SatCredentials, x509.Certificate]:
    if not cer or not key or len(cer) > MAX_FILE_BYTES or len(key) > MAX_FILE_BYTES:
        raise ValidationError("Certificado y llave: selecciona archivos .cer y .key de hasta 16 KB cada uno.")
    try:
        cert = x509.load_pem_x509_certificate(cer) if b"BEGIN CERTIFICATE" in cer else x509.load_der_x509_certificate(cer)
    except (ValueError, UnsupportedAlgorithm):
        raise ValidationError("Certificado .cer: el archivo no es un certificado válido.") from None
    now = timezone.now()
    if not cert.not_valid_before_utc <= now < cert.not_valid_after_utc:
        raise ValidationError("Certificado .cer: está caducado o todavía no inicia su vigencia.")
    rfc = (getattr(settings, "SAT_RFC", "") or "").strip().upper()
    if not rfc:
        raise ValidationError("Falta configurar el RFC de la empresa antes de activar una e.firma.")
    subjects = cert.subject.get_attributes_for_oid(NameOID.X500_UNIQUE_IDENTIFIER)
    if not any(rfc in re.split(r"[/\s]+", attribute.value.upper()) for attribute in subjects):
        raise ValidationError("Certificado .cer: el RFC no coincide con el de la empresa.")
    try:
        private_key = load_private_key(key, password)
    except (ValueError, TypeError, UnsupportedAlgorithm):
        raise ValidationError("Llave .key o contraseña: no se pudo abrir la llave privada.") from None
    public_key = cert.public_key()
    if not isinstance(private_key, rsa.RSAPrivateKey) or not isinstance(public_key, rsa.RSAPublicKey):
        raise ValidationError("Llave .key: la e.firma debe utilizar una llave RSA.")
    if private_key.key_size < 2048 or private_key.public_key().public_numbers() != public_key.public_numbers():
        raise ValidationError("Llave .key: no corresponde al certificado o su tamaño no es válido.")
    return SatCredentials("", "", password, rfc, cer, key), cert


def _safe_sat_error(exc: SatServiceError) -> str:
    if "revocado" in str(exc).lower() or "caduco" in str(exc).lower():
        return "El SAT rechazó el certificado como revocado o caduco. Revisa la e.firma vigente."
    return "No se pudo autenticar con el SAT. Revisa la e.firma e intenta nuevamente; el servicio puede estar temporalmente no disponible."


def _registrar_prueba(cert, *, ok, usuario):
    LogDescargaSat.objects.create(
        nivel=LogDescargaSat.NIVEL_INFO if ok else LogDescargaSat.NIVEL_ERROR,
        mensaje=f"Prueba e.firma SHA256 {cert.fingerprint(hashes.SHA256()).hex()}: "
                f"autenticación SAT {'verificada' if ok else 'fallida'} por usuario {usuario.pk}.")


@sensitive_variables()
def activar_efirma(credentials: SatCredentials, cert: x509.Certificate, *, usuario) -> None:
    try:
        obtener_token(credentials=credentials, timeout_seconds=20)
    except SatServiceError as exc:
        raise ValidationError(_safe_sat_error(exc)) from None
    data = {"cer": base64.b64encode(credentials.cer_data).decode("ascii"),
            "key": base64.b64encode(credentials.key_data).decode("ascii"),
            "password": credentials.password, "rfc": credentials.rfc}
    encrypted = _cipher().encrypt(json.dumps(data).encode())
    with transaction.atomic():
        ConfiguracionEfirma.objects.update_or_create(pk=1, defaults={
            "contenido_cifrado": encrypted, "rfc": credentials.rfc,
            "huella": cert.fingerprint(hashes.SHA256()).hex(),
            "vigente_desde": cert.not_valid_before_utc, "vigente_hasta": cert.not_valid_after_utc,
            "actualizado_por": usuario, "ultima_prueba_en": timezone.now(), "ultima_prueba_ok": True,
        })
        LogDescargaSat.objects.create(mensaje=f"e.firma actualizada y autenticación SAT verificada por usuario {usuario.pk}. No se solicitaron descargas.")


@sensitive_variables()
def probar_efirma(*, usuario) -> None:
    from sat_client.services.base import get_sat_credentials

    credentials = get_sat_credentials()
    cert = None
    try:
        cer = credentials.cer_data or Path(credentials.cer_path).read_bytes()
        key = credentials.key_data or Path(credentials.key_path).read_bytes()
        cert = x509.load_pem_x509_certificate(cer) if b"BEGIN CERTIFICATE" in cer else x509.load_der_x509_certificate(cer)
        credentials, cert = validar_efirma(cer, key, credentials.password)
        obtener_token(credentials=credentials, timeout_seconds=20)
    except (SatServiceError, ValidationError, OSError, ValueError, UnsupportedAlgorithm) as exc:
        if cert:
            ConfiguracionEfirma.objects.filter(pk=1, huella=cert.fingerprint(hashes.SHA256()).hex()).update(
                ultima_prueba_ok=False, ultima_prueba_en=timezone.now())
            _registrar_prueba(cert, ok=False, usuario=usuario)
        if isinstance(exc, ValidationError):
            raise
        if not isinstance(exc, SatServiceError):
            raise ValidationError("No se pueden leer los archivos de la e.firma configurada; revisa el certificado y la llave.") from None
        raise ValidationError(_safe_sat_error(exc)) from None
    ConfiguracionEfirma.objects.filter(pk=1, huella=cert.fingerprint(hashes.SHA256()).hex()).update(
        ultima_prueba_ok=True, ultima_prueba_en=timezone.now())
    _registrar_prueba(cert, ok=True, usuario=usuario)


def estado_efirma() -> dict:
    saved = ConfiguracionEfirma.objects.defer("contenido_cifrado").filter(pk=1).first()
    if saved:
        result = {"rfc": saved.rfc, "vigente_hasta": saved.vigente_hasta,
                  "caducado": not saved.vigente_desde <= timezone.now() < saved.vigente_hasta,
                  "ultima_prueba_ok": saved.ultima_prueba_ok, "ultima_prueba_en": saved.ultima_prueba_en,
                  "origen": "ERP"}
        try:
            cargar_efirma()
        except SatConfigurationError as exc:
            result.update(error=str(exc), ultima_prueba_ok=False)
        return result
    path = getattr(settings, "SAT_EFIRMA_CER_PATH", "")
    try:
        raw = Path(path).read_bytes() if path else b""
        cert = x509.load_pem_x509_certificate(raw) if b"BEGIN CERTIFICATE" in raw else x509.load_der_x509_certificate(raw)
    except (OSError, ValueError, UnsupportedAlgorithm):
        return {"rfc": getattr(settings, "SAT_RFC", ""), "origen": "Servidor"}
    result = {"rfc": getattr(settings, "SAT_RFC", ""), "origen": "Servidor", "vigente_hasta": cert.not_valid_after_utc,
              "caducado": not cert.not_valid_before_utc <= timezone.now() < cert.not_valid_after_utc}
    last_test = LogDescargaSat.objects.filter(
        mensaje__startswith=f"Prueba e.firma SHA256 {cert.fingerprint(hashes.SHA256()).hex()}: ").first()
    if last_test:
        result.update(ultima_prueba_en=last_test.creado_en, ultima_prueba_ok=last_test.nivel == LogDescargaSat.NIVEL_INFO)
    return result

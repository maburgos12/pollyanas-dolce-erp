import hashlib
import logging
from decimal import Decimal, InvalidOperation

from django.utils import timezone
from PIL import Image, ImageFilter, ImageStat, UnidentifiedImageError

from .models import CargaCombustibleUnidad

logger = logging.getLogger(__name__)


def auditar_carga_combustible(carga_id: int) -> dict:
    carga = CargaCombustibleUnidad.objects.select_related("bitacora", "unidad", "repartidor__user").get(pk=carga_id)
    motivos: list[str] = []
    score = 0

    ticket_sha = _sha256_field_file(carga.foto_ticket)
    if ticket_sha:
        duplicada = (
            CargaCombustibleUnidad.objects.filter(ticket_sha256=ticket_sha)
            .exclude(pk=carga.pk)
            .exists()
        )
        if duplicada:
            score += 75
            motivos.append("ticket_duplicado")

    precio_litro = _precio_litro(carga)
    if precio_litro is not None and (precio_litro < Decimal("18") or precio_litro > Decimal("35")):
        score += 30
        motivos.append("precio_por_litro_fuera_de_rango")
    if carga.litros > Decimal("80"):
        score += 35
        motivos.append("litros_muy_altos")
    if carga.importe_total >= Decimal("1000") and carga.importe_total % Decimal("100") == 0:
        score += 10
        motivos.append("importe_redondo_alto")

    imagen = _analizar_imagen(carga.foto_ticket)
    score += imagen["score"]
    motivos.extend(imagen["motivos"])

    estado = _estado(score)
    carga.ticket_sha256 = ticket_sha or ""
    carga.auditoria_score = min(score, 100)
    carga.auditoria_estado = estado
    carga.auditoria_motivos = motivos
    carga.auditoria_detalle = {
        "modo": "reglas_locales",
        "precio_litro": str(precio_litro) if precio_litro is not None else None,
        "imagen": imagen["detalle"],
    }
    carga.auditoria_analizada_en = timezone.now()
    carga.save(
        update_fields=[
            "ticket_sha256",
            "auditoria_score",
            "auditoria_estado",
            "auditoria_motivos",
            "auditoria_detalle",
            "auditoria_analizada_en",
        ]
    )
    return {"estado": estado, "score": min(score, 100), "motivos": motivos}


def _estado(score: int) -> str:
    if score >= 70:
        return CargaCombustibleUnidad.AUDITORIA_ALTO_RIESGO
    if score >= 25:
        return CargaCombustibleUnidad.AUDITORIA_REVISION
    return CargaCombustibleUnidad.AUDITORIA_OK


def _sha256_field_file(field_file) -> str:
    digest = hashlib.sha256()
    field_file.open("rb")
    try:
        for chunk in field_file.chunks():
            digest.update(chunk)
    finally:
        field_file.close()
    return digest.hexdigest()


def _precio_litro(carga: CargaCombustibleUnidad) -> Decimal | None:
    try:
        return carga.importe_total / carga.litros
    except (InvalidOperation, ZeroDivisionError):
        return None


def _analizar_imagen(field_file) -> dict:
    motivos: list[str] = []
    score = 0
    detalle = {"status": "ok"}

    if getattr(field_file, "size", 0) and field_file.size < 15_000:
        score += 20
        motivos.append("archivo_muy_chico")

    field_file.open("rb")
    try:
        with Image.open(field_file) as image:
            width, height = image.size
            gray = image.convert("L")
            stat = ImageStat.Stat(gray)
            brightness = stat.mean[0]
            contrast = stat.stddev[0]
            edge_detail = ImageStat.Stat(gray.filter(ImageFilter.FIND_EDGES)).mean[0]
    except (UnidentifiedImageError, OSError) as exc:
        logger.warning("No se pudo leer imagen de ticket combustible: %s", exc)
        return {"score": 70, "motivos": ["imagen_no_legible"], "detalle": {"status": "error"}}
    finally:
        field_file.close()

    shortest = min(width, height)
    longest = max(width, height)
    ratio = longest / shortest if shortest else 0
    detalle.update(
        {
            "width": width,
            "height": height,
            "brightness": round(brightness, 2),
            "contrast": round(contrast, 2),
            "edge_detail": round(edge_detail, 2),
        }
    )

    if shortest < 480:
        score += 25
        motivos.append("imagen_muy_chica")
    if width > height and (width / height) > 1.25:
        score += 25
        motivos.append("imagen_horizontal")
    if ratio < 1.2:
        score += 15
        motivos.append("imagen_casi_cuadrada")
    if brightness < 35:
        score += 25
        motivos.append("imagen_muy_oscura")
    if contrast < 18:
        score += 20
        motivos.append("imagen_bajo_contraste")
    if edge_detail < 4:
        score += 20
        motivos.append("imagen_sin_detalle")

    return {"score": score, "motivos": motivos, "detalle": detalle}

import hashlib
import logging
from decimal import Decimal, InvalidOperation

from django.utils import timezone
from PIL import Image, ImageOps, ImageStat, UnidentifiedImageError

from .models import CargaCombustibleUnidad
from .services_ticket_ocr import TicketOCRNoDisponible, leer_ticket

logger = logging.getLogger(__name__)

# El importe es lo que se audita: es el dinero que salió y el que viene en el vale.
TOLERANCIA_IMPORTE = Decimal("5.00")
# Los repartidores teclean los litros redondeados (44.00 por 44.61) mientras el
# importe coincide exacto. Eso es captura descuidada, no fraude: se informa sin
# sumar riesgo. Solo un desvío grande con el importe también descuadrado importa.
TOLERANCIA_LITROS_AVISO = Decimal("0.01")   # 1%: en producción el redondeo anda en 1-2%
# Margen para dar por buena la aritmética del propio ticket (litros x precio).
TOLERANCIA_COHERENCIA = Decimal("0.02")


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

    imagen = _analizar_imagen(carga.foto_ticket)
    score += imagen["score"]
    motivos.extend(imagen["motivos"])

    lectura = _cotejar_ticket(carga)
    score += lectura["score"]
    motivos.extend(lectura["motivos"])

    estado = _estado(score)
    carga.ticket_sha256 = ticket_sha or ""
    carga.auditoria_score = min(score, 100)
    carga.auditoria_estado = estado
    carga.auditoria_motivos = motivos
    carga.auditoria_detalle = {
        "modo": lectura["modo"],
        "precio_litro": str(precio_litro) if precio_litro is not None else None,
        "imagen": imagen["detalle"],
        "ticket_leido": lectura["detalle"],
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
    return {"estado": estado, "score": min(score, 100), "motivos": motivos, "ticket": lectura["detalle"]}


def _cotejar_ticket(carga: CargaCombustibleUnidad) -> dict:
    """Lee el ticket y lo compara contra lo que capturó el repartidor.

    Si la lectura no está disponible, la carga NO se castiga: se marca para
    revisión humana sin sumar puntos de riesgo.
    """
    try:
        leido = leer_ticket(carga.foto_ticket)
    except TicketOCRNoDisponible as exc:
        logger.warning("No se pudo leer ticket de carga %s: %s", carga.pk, exc)
        return {
            "score": 0,
            "motivos": ["lectura_no_disponible"],
            "modo": "reglas_locales",
            "detalle": {"status": "no_disponible", "error": str(exc)},
        }

    motivos: list[str] = []
    score = 0
    detalle = dict(leido, status="ok")
    detalle["litros"] = str(leido["litros"]) if leido["litros"] is not None else None
    detalle["importe_total"] = str(leido["importe_total"]) if leido["importe_total"] is not None else None
    detalle["precio_por_litro"] = (
        str(leido["precio_por_litro"]) if leido["precio_por_litro"] is not None else None
    )

    if not leido["es_ticket"]:
        return {
            "score": 80,
            "motivos": ["foto_no_es_ticket"],
            "modo": "ocr_vision",
            "detalle": detalle,
        }

    if not leido["legible"]:
        return {
            "score": 0,
            "motivos": ["ticket_ilegible"],
            "modo": "ocr_vision",
            "detalle": detalle,
        }

    # El folio caza el mismo ticket refotografiado; el SHA256 solo caza el mismo archivo.
    if leido["folio"] and _folio_repetido(leido["folio"], carga.pk):
        score += 80
        motivos.append("folio_repetido")

    # El importe es la señal dura: es el dinero que salió.
    diferencia_importe = _diferencia(leido["importe_total"], carga.importe_total)
    if diferencia_importe is None:
        motivos.append("ticket_sin_importe")
    elif diferencia_importe > TOLERANCIA_IMPORTE:
        score += 45
        motivos.append("importe_no_coincide")
    detalle["diferencia_importe"] = str(diferencia_importe) if diferencia_importe is not None else None

    # Los litros solo valen si la aritmética del propio ticket cuadra. Un ticket
    # doblado en ese renglón hace que el lector devuelva un número inventado.
    coherente = _ticket_coherente(leido)
    detalle["lectura_coherente"] = coherente
    diferencia_litros = _diferencia(leido["litros"], carga.litros)
    detalle["diferencia_litros"] = str(diferencia_litros) if diferencia_litros is not None else None

    if not coherente:
        motivos.append("litros_del_ticket_dudosos")
    elif diferencia_litros is None:
        motivos.append("ticket_sin_litros")
    elif carga.litros and (diferencia_litros / carga.litros) > TOLERANCIA_LITROS_AVISO:
        # Sin score: si el dinero cuadra, un litraje mal tecleado no es un riesgo.
        motivos.append("litros_capturados_difieren")

    if score == 0:
        # El dinero del ticket cuadra con lo capturado; los avisos de litros van después.
        motivos.insert(0, "ticket_verificado")

    return {"score": score, "motivos": motivos, "modo": "ocr_vision", "detalle": detalle}


def _ticket_coherente(leido: dict) -> bool:
    """¿La aritmética del propio ticket cuadra (litros x precio = importe)?

    Si no cuadra, el lector se inventó alguna cifra —típicamente cuando el
    renglón viene doblado— y sus litros no sirven para acusar a nadie.
    """
    litros, precio, importe = leido["litros"], leido["precio_por_litro"], leido["importe_total"]
    if not litros or not precio or not importe:
        return False
    esperado = litros * precio
    return abs(esperado - importe) <= importe * TOLERANCIA_COHERENCIA


def _folio_repetido(folio: str, carga_id: int) -> bool:
    """¿Otro registro ya declaró este folio de ticket?"""
    # ponytail: consulta sobre el JSONField, sin índice. Con ~30 cargas al mes
    # sobra; si el volumen crece, promover folio a columna indexada.
    return (
        CargaCombustibleUnidad.objects.filter(
            auditoria_detalle__ticket_leido__folio=folio
        )
        .exclude(pk=carga_id)
        .exists()
    )


def _diferencia(leido: Decimal | None, capturado: Decimal | None) -> Decimal | None:
    if leido is None or capturado is None:
        return None
    return abs(Decimal(leido) - Decimal(capturado))


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
    """Solo verifica que la foto se pueda abrir y no esté a oscuras.

    La forma de la foto (vertical, cuadrada, proporción) NO dice nada sobre si
    es un ticket: de eso se encarga la lectura del ticket.
    """
    motivos: list[str] = []
    score = 0
    detalle = {"status": "ok"}

    field_file.open("rb")
    try:
        with Image.open(field_file) as original:
            imagen = ImageOps.exif_transpose(original)
            width, height = imagen.size
            gris = imagen.convert("L")
            stat = ImageStat.Stat(gris)
            brightness = stat.mean[0]
            contrast = stat.stddev[0]
    except (UnidentifiedImageError, OSError) as exc:
        logger.warning("No se pudo leer imagen de ticket combustible: %s", exc)
        return {"score": 70, "motivos": ["imagen_no_legible"], "detalle": {"status": "error"}}
    finally:
        field_file.close()

    detalle.update(
        {
            "width": width,
            "height": height,
            "brightness": round(brightness, 2),
            "contrast": round(contrast, 2),
        }
    )

    if brightness < 35:
        score += 25
        motivos.append("imagen_muy_oscura")

    return {"score": score, "motivos": motivos, "detalle": detalle}

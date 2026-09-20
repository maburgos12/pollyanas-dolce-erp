"""Frontera documental y transaccional para expedientes de cédulas IMSS."""

from __future__ import annotations

import hashlib
import mimetypes
import os
import re
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from django.core.files.base import ContentFile
from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from rrhh.models import Empleado

from .models import (
    DetalleCedulaIMSS,
    DocumentoCedulaIMSS,
    ExpedienteCedulaIMSS,
    LineaPresupuestoMensual,
)
from .services_cedula_imss import (
    AREA_RESPALDO,
    DEPARTAMENTO_A_AREA,
    FUENTE_SIPARE,
    CedulaParseada,
    _columnas_montos,
    _decimal,
    _es_numero,
    _es_totalizador,
    _nss_digits,
    _valor_columna,
    aplicar_cedula,
    cargar_filas_xls,
    parsear_cedula,
)
from .services_presupuesto_maestro import normalize_header_text


class CedulaDiscrepante(ValueError):
    """La suma patronal del detalle no coincide con el control impreso."""


@dataclass(frozen=True)
class DocumentoPreview:
    clase: str
    nombre_original: str
    contenido: bytes
    sha256: str
    tamano: int
    mime_type: str
    total_visible: Decimal | None = None


@dataclass(frozen=True)
class DetallePreview:
    nss: str
    nombre_origen: str
    dias: Decimal
    sdi: Decimal
    retiro: Decimal
    cesantia_patronal: Decimal
    aportacion_vivienda: Decimal
    cuota_patronal: Decimal
    empleado_id: int | None
    area_codigo: str
    sucursal_id: int | None
    cruce_estado: str


@dataclass(frozen=True)
class PreviewExpediente:
    parseada: CedulaParseada
    documentos: tuple[DocumentoPreview, ...]
    detalles: tuple[DetallePreview, ...]
    total_detalle: Decimal
    total_patronal: Decimal
    nss_duplicados: tuple[str, ...] = ()

    @property
    def sua(self) -> DocumentoPreview:
        return next(d for d in self.documentos if d.clase == DocumentoCedulaIMSS.CLASE_SUA_XLS)


def _leer_archivo(archivo) -> bytes:
    if hasattr(archivo, "seek"):
        archivo.seek(0)
    if hasattr(archivo, "chunks"):
        contenido = b"".join(archivo.chunks())
    else:
        contenido = archivo.read()
    if hasattr(archivo, "seek"):
        archivo.seek(0)
    return contenido


def _clasificar_pdf(nombre: str, contenido: bytes) -> str:
    texto = normalize_header_text(f"{nombre} {contenido[:200_000].decode('latin-1', errors='ignore')}")
    ema = bool(re.search(r"\bema\b", texto)) or "emision mensual anticipada" in texto
    eba = bool(re.search(r"\beba\b", texto)) or "emision bimestral anticipada" in texto
    if ema == eba:
        raise ValueError(f"No se pudo clasificar inequívocamente el PDF '{nombre}' como EMA o EBA.")
    return DocumentoCedulaIMSS.CLASE_EMA_PDF if ema else DocumentoCedulaIMSS.CLASE_EBA_PDF


def _documento_preview(archivo) -> DocumentoPreview:
    nombre = Path(str(getattr(archivo, "name", ""))).name
    extension = Path(nombre).suffix.lower()
    contenido = _leer_archivo(archivo)
    if not contenido:
        raise ValueError(f"El archivo '{nombre}' está vacío.")
    if extension == ".xls":
        if not contenido.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
            raise ValueError(f"El archivo '{nombre}' no tiene una firma válida de Excel .xls.")
        clase = DocumentoCedulaIMSS.CLASE_SUA_XLS
    elif extension == ".pdf":
        if not contenido.startswith(b"%PDF-"):
            raise ValueError(f"El archivo '{nombre}' no tiene una firma válida de PDF.")
        clase = _clasificar_pdf(nombre, contenido)
    else:
        raise ValueError(f"Extensión no permitida en '{nombre}'; solo se aceptan .xls y .pdf.")
    mime = getattr(archivo, "content_type", "") or mimetypes.guess_type(nombre)[0] or "application/octet-stream"
    return DocumentoPreview(
        clase=clase,
        nombre_original=nombre,
        contenido=contenido,
        sha256=hashlib.sha256(contenido).hexdigest(),
        tamano=len(contenido),
        mime_type=mime,
    )


def _filas_sua(documento: DocumentoPreview) -> list[list[object]]:
    temporal = tempfile.NamedTemporaryFile(suffix=".xls", delete=False)
    try:
        temporal.write(documento.contenido)
        temporal.close()
        return cargar_filas_xls(temporal.name)
    finally:
        temporal.close()
        os.unlink(temporal.name)


def _extraer_total_control(filas: list[list[object]], tipo: str) -> Decimal:
    """Lee el total patronal impreso; nunca deriva el control del detalle."""
    _, columnas = _columnas_montos(filas, tipo)
    claves = ["patronal"] if tipo == ExpedienteCedulaIMSS.TIPO_MENSUAL else [
        "retiro", "cv_patronal", "aportacion"
    ]
    candidatos: set[Decimal] = set()
    for fila in filas:
        if not _es_totalizador(fila):
            continue
        valores = [_valor_columna(fila, columnas[clave]) for clave in claves]
        if all(_es_numero(valor) for valor in valores):
            candidatos.add(sum((_decimal(valor) for valor in valores), Decimal("0")).quantize(Decimal("0.01")))
    if len(candidatos) != 1:
        raise CedulaDiscrepante(
            "El XLS no contiene un total patronal de control inequívoco "
            f"(se encontraron {len(candidatos)} valores)."
        )
    return candidatos.pop()


def _cruzar_detalles(parseada: CedulaParseada) -> tuple[tuple[DetallePreview, ...], tuple[str, ...]]:
    por_nss: dict[str, list[Empleado]] = {}
    for empleado in Empleado.objects.filter(activo=True).select_related("sucursal_ref"):
        nss = _nss_digits(empleado.nss)
        if nss:
            por_nss.setdefault(nss, []).append(empleado)
    nss_cedula = {trabajador.nss for trabajador in parseada.trabajadores}
    duplicados = tuple(sorted(nss for nss, empleados in por_nss.items() if len(empleados) > 1 and nss in nss_cedula))
    detalles: list[DetallePreview] = []
    for trabajador in parseada.trabajadores:
        coincidencias = por_nss.get(trabajador.nss, [])
        empleado = coincidencias[0] if len(coincidencias) == 1 else None
        area = ""
        sucursal_id = None
        if empleado is not None:
            area = DEPARTAMENTO_A_AREA.get((empleado.departamento or "").upper(), AREA_RESPALDO)
            sucursal_id = empleado.sucursal_ref_id if area == "gastos-venta" else None
        detalles.append(DetallePreview(
            nss=trabajador.nss,
            nombre_origen=trabajador.nombre,
            dias=trabajador.dias,
            sdi=trabajador.sdi,
            retiro=trabajador.retiro,
            cesantia_patronal=trabajador.cesantia_patronal,
            aportacion_vivienda=trabajador.aportacion_vivienda,
            cuota_patronal=trabajador.patronal,
            empleado_id=empleado.pk if empleado else None,
            area_codigo=area,
            sucursal_id=sucursal_id,
            cruce_estado=(
                DetalleCedulaIMSS.CRUCE_CRUZADO if empleado else DetalleCedulaIMSS.CRUCE_SIN_CRUCE
            ),
        ))
    return tuple(detalles), duplicados


def preparar_expediente(archivos: Iterable, usuario=None) -> PreviewExpediente:
    """Valida y previsualiza una cédula y sus evidencias sin persistir nada."""
    documentos = tuple(_documento_preview(archivo) for archivo in archivos)
    sua = [documento for documento in documentos if documento.clase == DocumentoCedulaIMSS.CLASE_SUA_XLS]
    if len(sua) != 1:
        raise ValueError(f"Se requiere exactamente un archivo SUA .xls; se recibieron {len(sua)}.")
    filas = _filas_sua(sua[0])
    parseada = parsear_cedula(filas)
    if not parseada.registro_patronal.strip():
        raise ValueError("El XLS no contiene un registro patronal inequívoco.")
    total_control = _extraer_total_control(filas, parseada.tipo)
    detalles, duplicados = _cruzar_detalles(parseada)
    total_detalle = sum((detalle.cuota_patronal for detalle in detalles), Decimal("0")).quantize(
        Decimal("0.01")
    )
    documentos_con_total = tuple(
        DocumentoPreview(
            clase=documento.clase,
            nombre_original=documento.nombre_original,
            contenido=documento.contenido,
            sha256=documento.sha256,
            tamano=documento.tamano,
            mime_type=documento.mime_type,
            total_visible=total_control if documento.clase == DocumentoCedulaIMSS.CLASE_SUA_XLS else None,
        )
        for documento in documentos
    )
    return PreviewExpediente(
        parseada=parseada,
        documentos=documentos_con_total,
        detalles=detalles,
        total_detalle=total_detalle,
        total_patronal=total_control,
        nss_duplicados=duplicados,
    )


def _validar_nss_actuales(preview: PreviewExpediente) -> None:
    _, duplicados = _cruzar_detalles(preview.parseada)
    if duplicados:
        raise ValueError(f"NSS duplicados activos en RRHH: {', '.join(duplicados)}. Corrige los expedientes.")


def _enlazar_lineas(preview: PreviewExpediente, expediente, documento_sua) -> None:
    for linea in LineaPresupuestoMensual.objects.filter(
        periodo__in=preview.parseada.meses,
        fuente_real=FUENTE_SIPARE,
    ):
        cedula = (linea.metadata or {}).get("cedula_imss", {})
        if cedula.get("tipo") != preview.parseada.tipo:
            continue
        if cedula.get("registro_patronal") != preview.parseada.registro_patronal:
            continue
        metadata = dict(linea.metadata or {})
        metadata["expediente_cedula_imss_id"] = expediente.pk
        metadata["documento_cedula_imss_id"] = documento_sua.pk
        LineaPresupuestoMensual.objects.filter(pk=linea.pk).update(metadata=metadata)


def aplicar_expediente(preview: PreviewExpediente, usuario):
    """Persiste y materializa una previsualización de forma atómica e idempotente."""
    existente = (
        DocumentoCedulaIMSS.objects.filter(sha256=preview.sua.sha256)
        .select_related("expediente")
        .first()
    )
    if existente:
        return existente.expediente
    if preview.total_detalle != preview.total_patronal:
        raise CedulaDiscrepante(
            f"Detalle {preview.total_detalle} != control patronal {preview.total_patronal}."
        )
    _validar_nss_actuales(preview)

    blobs_guardados: list[tuple[object, str]] = []
    try:
        with transaction.atomic():
            expediente = ExpedienteCedulaIMSS.objects.create(
                tipo=preview.parseada.tipo,
                periodo=preview.parseada.periodo,
                registro_patronal=preview.parseada.registro_patronal,
                estado=ExpedienteCedulaIMSS.ESTADO_VALIDO,
                total_patronal=preview.total_patronal,
                trabajadores=len(preview.detalles),
                cruzados=sum(d.empleado_id is not None for d in preview.detalles),
                sin_cruce=sum(d.empleado_id is None for d in preview.detalles),
                aplicado_por=usuario,
                metadata={"sha256_sua": preview.sua.sha256},
            )
            documentos: dict[str, DocumentoCedulaIMSS] = {}
            for documento_preview in preview.documentos:
                documento = DocumentoCedulaIMSS(
                    expediente=expediente,
                    clase=documento_preview.clase,
                    nombre_original=documento_preview.nombre_original,
                    sha256=documento_preview.sha256,
                    tamano=documento_preview.tamano,
                    mime_type=documento_preview.mime_type,
                    total_visible=documento_preview.total_visible,
                    metadata={"firma_validada": True},
                )
                documento.archivo.save(
                    documento_preview.nombre_original,
                    ContentFile(documento_preview.contenido),
                    save=False,
                )
                blobs_guardados.append((documento.archivo.storage, documento.archivo.name))
                documento.save()
                documentos[documento_preview.sha256] = documento

            documento_sua = documentos[preview.sua.sha256]
            DetalleCedulaIMSS.objects.bulk_create([
                DetalleCedulaIMSS(
                    documento=documento_sua,
                    empleado_id=detalle.empleado_id,
                    nss=detalle.nss,
                    nombre_origen=detalle.nombre_origen,
                    dias=detalle.dias,
                    sdi=detalle.sdi,
                    retiro=detalle.retiro,
                    cesantia_patronal=detalle.cesantia_patronal,
                    aportacion_vivienda=detalle.aportacion_vivienda,
                    cuota_patronal=detalle.cuota_patronal,
                    area_codigo=detalle.area_codigo,
                    sucursal_id=detalle.sucursal_id,
                    cruce_estado=detalle.cruce_estado,
                )
                for detalle in preview.detalles
            ])
            resumen = aplicar_cedula(preview.parseada)
            _enlazar_lineas(preview, expediente, documento_sua)
            expediente.estado = ExpedienteCedulaIMSS.ESTADO_APLICADO
            expediente.aplicado_en = timezone.now()
            expediente.metadata = {
                **expediente.metadata,
                "lineas_actualizadas": resumen.lineas_actualizadas,
                "protegidas_manual": resumen.protegidas_manual,
                "avisos": resumen.avisos,
            }
            expediente.save(update_fields=["estado", "aplicado_en", "metadata"])
            log_event(
                usuario,
                "CEDULA_IMSS_APLICADA",
                "reportes.ExpedienteCedulaIMSS",
                str(expediente.pk),
                {
                    "tipo": expediente.tipo,
                    "periodo": expediente.periodo.isoformat(),
                    "registro_patronal": expediente.registro_patronal,
                    "total_patronal": str(expediente.total_patronal),
                    "documentos": len(preview.documentos),
                    "trabajadores": len(preview.detalles),
                },
            )
        return expediente
    except Exception:
        for storage, nombre in reversed(blobs_guardados):
            storage.delete(nombre)
        raise

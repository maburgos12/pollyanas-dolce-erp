"""Frontera documental y transaccional para expedientes de cédulas IMSS."""

from __future__ import annotations

import hashlib
from io import BytesIO
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from django.core.files.base import ContentFile
from django.db import IntegrityError, transaction
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
    _rubro_destino,
    cargar_filas_xls,
    parsear_cedula,
)
from .services_presupuesto_maestro import normalize_header_text


class CedulaDiscrepante(ValueError):
    """La suma patronal del detalle no coincide con el control impreso."""


logger = logging.getLogger(__name__)
MAX_ARCHIVO_BYTES = 10 * 1024 * 1024
MAX_EXPEDIENTE_BYTES = 30 * 1024 * 1024


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


def _leer_archivo(archivo, nombre: str) -> bytes:
    if hasattr(archivo, "seek"):
        archivo.seek(0)
    partes: list[bytes] = []
    tamano = 0
    if hasattr(archivo, "chunks"):
        for parte in archivo.chunks():
            tamano += len(parte)
            if tamano > MAX_ARCHIVO_BYTES:
                raise ValueError(f"El archivo '{nombre}' excede el límite de 10 MiB.")
            partes.append(parte)
        contenido = b"".join(partes)
    else:
        contenido = archivo.read(MAX_ARCHIVO_BYTES + 1)
        if len(contenido) > MAX_ARCHIVO_BYTES:
            raise ValueError(f"El archivo '{nombre}' excede el límite de 10 MiB.")
    if hasattr(archivo, "seek"):
        archivo.seek(0)
    return contenido


def _clasificar_pdf(nombre: str, contenido: bytes) -> str:
    try:
        from pypdf import PdfReader

        lector = PdfReader(BytesIO(contenido), strict=False)
        if lector.is_encrypted or len(lector.pages) < 1:
            raise ValueError
        texto = normalize_header_text(" ".join(pagina.extract_text() or "" for pagina in lector.pages))
    except Exception as exc:
        raise ValueError(f"El archivo '{nombre}' no es un PDF válido y parseable.") from exc
    ema = bool(re.search(r"\bema\b", texto)) or "emision mensual anticipada" in texto
    eba = bool(re.search(r"\beba\b", texto)) or "emision bimestral anticipada" in texto
    if ema == eba:
        raise ValueError(f"No se pudo clasificar inequívocamente el PDF '{nombre}' como EMA o EBA.")
    return DocumentoCedulaIMSS.CLASE_EMA_PDF if ema else DocumentoCedulaIMSS.CLASE_EBA_PDF


def _documento_preview(archivo) -> DocumentoPreview:
    nombre = Path(str(getattr(archivo, "name", ""))).name
    extension = Path(nombre).suffix.lower()
    contenido = _leer_archivo(archivo, nombre)
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
    mime = "application/vnd.ms-excel" if clase == DocumentoCedulaIMSS.CLASE_SUA_XLS else "application/pdf"
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


def _cruzar_detalles(
    parseada: CedulaParseada, *, bloquear: bool = False
) -> tuple[tuple[DetallePreview, ...], tuple[str, ...]]:
    por_nss: dict[str, list[Empleado]] = {}
    empleados = Empleado.objects.filter(activo=True).select_related("sucursal_ref")
    if bloquear:
        empleados = empleados.select_for_update(of=("self",))
    for empleado in empleados:
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
    if sum(documento.tamano for documento in documentos) > MAX_EXPEDIENTE_BYTES:
        raise ValueError("El expediente excede el límite total de 30 MiB.")
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


@dataclass(frozen=True)
class ResultadoMaterializacion:
    actualizadas: int
    protegidas_manual: int
    conflictos_auto: int
    avisos: tuple[str, ...]


def _guardar_documento(expediente, documento_preview, blobs_guardados):
    documento = DocumentoCedulaIMSS(
        expediente=expediente,
        clase=documento_preview.clase,
        nombre_original=documento_preview.nombre_original,
        sha256=documento_preview.sha256,
        tamano=documento_preview.tamano,
        mime_type=documento_preview.mime_type,
        total_visible=documento_preview.total_visible,
        metadata={
            "validado_con": "xlrd" if documento_preview.clase == DocumentoCedulaIMSS.CLASE_SUA_XLS else "pypdf",
        },
    )
    documento.archivo.save(
        documento_preview.nombre_original,
        ContentFile(documento_preview.contenido),
        save=False,
    )
    blobs_guardados.append((documento.archivo.storage, documento.archivo.name))
    documento.save()
    return documento


def _adjuntar_pdfs(expediente, preview, blobs_guardados) -> list[DocumentoCedulaIMSS]:
    existentes = set(
        DocumentoCedulaIMSS.objects.filter(
            sha256__in=[d.sha256 for d in preview.documentos]
        ).values_list("sha256", flat=True)
    )
    agregados = []
    for documento in preview.documentos:
        if documento.clase == DocumentoCedulaIMSS.CLASE_SUA_XLS or documento.sha256 in existentes:
            continue
        agregados.append(_guardar_documento(expediente, documento, blobs_guardados))
        existentes.add(documento.sha256)
    return agregados


def _crear_detalles(documento_sua, detalles: tuple[DetallePreview, ...]) -> None:
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
        for detalle in detalles
    ])


def _materializar_desde_detalles(
    preview: PreviewExpediente,
    detalles: tuple[DetallePreview, ...],
    expediente,
    documento_sua,
) -> ResultadoMaterializacion:
    totales: dict[tuple[str, int | None], Decimal] = {}
    avisos: list[str] = []
    for detalle in detalles:
        if detalle.empleado_id is None:
            continue
        clave = (detalle.area_codigo, detalle.sucursal_id)
        totales[clave] = totales.get(clave, Decimal("0")) + detalle.cuota_patronal
    totales[("nomina", None)] = preview.total_patronal
    conceptos = ["imss"] if preview.parseada.tipo == "MENSUAL" else ["infonavit rcv", "infonavit"]
    destinos: dict[tuple[int, object], tuple[object, Decimal]] = {}
    for (area_codigo, sucursal_id), total in sorted(totales.items(), key=lambda item: (item[0][0], item[0][1] or 0)):
        rubro = _rubro_destino(area_codigo, sucursal_id, conceptos, avisos)
        if rubro is None:
            continue
        meses = preview.parseada.meses
        primera = (total / Decimal(len(meses))).quantize(Decimal("0.01"))
        montos = [primera] * (len(meses) - 1) + [total - primera * (len(meses) - 1)]
        for mes, monto in zip(meses, montos):
            clave = (rubro.pk, mes)
            previo = destinos.get(clave, (rubro, Decimal("0")))[1]
            destinos[clave] = (rubro, previo + monto)

    actualizadas = protegidas = conflictos = 0
    cambios = []
    for (_, mes), (rubro, monto) in destinos.items():
        linea, _ = LineaPresupuestoMensual.objects.get_or_create(
            rubro=rubro,
            periodo=mes,
            version=LineaPresupuestoMensual.VERSION_ORIGINAL,
            defaults={"monto_presupuesto": Decimal("0")},
        )
        linea = LineaPresupuestoMensual.objects.select_for_update().get(pk=linea.pk)
        fuente = str(linea.fuente_real or "")
        if fuente.startswith("MANUAL:"):
            protegidas += 1
            continue
        if fuente and fuente not in (FUENTE_SIPARE, "AUTO:LEGADO"):
            conflictos += 1
            avisos.append(f"{rubro} {mes:%Y-%m}: conflicto con {fuente}; no se pisó")
            continue
        metadata = dict(linea.metadata or {})
        metadata["cedula_imss"] = {
            "tipo": preview.parseada.tipo,
            "registro_patronal": preview.parseada.registro_patronal,
            "trabajadores": sum(d.empleado_id is not None for d in detalles),
            "importado_en": timezone.now().isoformat(),
        }
        metadata["expediente_cedula_imss_id"] = expediente.pk
        metadata["documento_cedula_imss_id"] = documento_sua.pk
        metadata.pop("sin_datos_fuente", None)
        metadata.pop("fuente_sin_datos_en", None)
        linea.monto_real = monto
        linea.fuente_real = FUENTE_SIPARE
        linea.metadata = metadata
        linea.actualizado_en = timezone.now()
        cambios.append(linea)
        actualizadas += 1
    if cambios:
        LineaPresupuestoMensual.objects.bulk_update(
            cambios, ["monto_real", "fuente_real", "metadata", "actualizado_en"]
        )
    return ResultadoMaterializacion(actualizadas, protegidas, conflictos, tuple(avisos))


def _limpiar_blobs(blobs_guardados, *, protegidos=frozenset()) -> None:
    """Limpia nombres confirmados sin ocultar el error original.

    Un backend que persista y falle antes de devolver/asignar el nombre incumple
    el contrato observable de Storage.save; ese blob no puede descubrirse aquí.
    """
    for storage, nombre in reversed(blobs_guardados):
        if nombre in protegidos:
            continue
        try:
            storage.delete(nombre)
        except Exception:
            logger.warning("No se pudo limpiar blob de cédula IMSS: %s", nombre, exc_info=True)


def aplicar_expediente(preview: PreviewExpediente, usuario):
    """Persiste y materializa una previsualización de forma atómica e idempotente."""
    if preview.total_detalle != preview.total_patronal:
        raise CedulaDiscrepante(
            f"Detalle {preview.total_detalle} != control patronal {preview.total_patronal}."
        )
    blobs_guardados: list[tuple[object, str]] = []
    try:
        with transaction.atomic():
            documento_sua = (
                DocumentoCedulaIMSS.objects.select_for_update()
                .filter(sha256=preview.sua.sha256, clase=DocumentoCedulaIMSS.CLASE_SUA_XLS)
                .select_related("expediente")
                .first()
            )
            if documento_sua is not None:
                expediente = ExpedienteCedulaIMSS.objects.select_for_update().get(
                    pk=documento_sua.expediente_id
                )
                _adjuntar_pdfs(expediente, preview, blobs_guardados)
                if expediente.estado in {
                    ExpedienteCedulaIMSS.ESTADO_APLICADO,
                    ExpedienteCedulaIMSS.ESTADO_REEMPLAZADO,
                }:
                    return expediente
            else:
                revisiones = list(
                    ExpedienteCedulaIMSS.objects.select_for_update().filter(
                        tipo=preview.parseada.tipo,
                        periodo=preview.parseada.periodo,
                        registro_patronal=preview.parseada.registro_patronal,
                    )
                )
                revision = max((e.revision for e in revisiones), default=0) + 1
                ExpedienteCedulaIMSS.objects.filter(
                    pk__in=[e.pk for e in revisiones if e.estado == ExpedienteCedulaIMSS.ESTADO_APLICADO]
                ).update(estado=ExpedienteCedulaIMSS.ESTADO_REEMPLAZADO)
                expediente = ExpedienteCedulaIMSS.objects.create(
                    tipo=preview.parseada.tipo,
                    periodo=preview.parseada.periodo,
                    registro_patronal=preview.parseada.registro_patronal,
                    revision=revision,
                    estado=ExpedienteCedulaIMSS.ESTADO_VALIDO,
                    total_patronal=preview.total_patronal,
                    aplicado_por=usuario,
                    metadata={"sha256_sua": preview.sua.sha256},
                )
                documento_sua = _guardar_documento(expediente, preview.sua, blobs_guardados)
                _adjuntar_pdfs(expediente, preview, blobs_guardados)

            revisiones = list(
                ExpedienteCedulaIMSS.objects.select_for_update().filter(
                    tipo=preview.parseada.tipo,
                    periodo=preview.parseada.periodo,
                    registro_patronal=preview.parseada.registro_patronal,
                )
            )
            ExpedienteCedulaIMSS.objects.filter(
                pk__in=[
                    e.pk for e in revisiones
                    if e.pk != expediente.pk and e.estado == ExpedienteCedulaIMSS.ESTADO_APLICADO
                ]
            ).update(estado=ExpedienteCedulaIMSS.ESTADO_REEMPLAZADO)
            detalles, duplicados = _cruzar_detalles(preview.parseada, bloquear=True)
            if duplicados:
                raise ValueError(
                    f"NSS duplicados activos en RRHH: {', '.join(duplicados)}. Corrige los expedientes."
                )
            if documento_sua.detalles.exists():
                documento_sua.detalles.all().delete()
            _crear_detalles(documento_sua, detalles)
            resultado = _materializar_desde_detalles(
                preview, detalles, expediente, documento_sua
            )
            expediente.estado = ExpedienteCedulaIMSS.ESTADO_APLICADO
            expediente.aplicado_en = timezone.now()
            expediente.aplicado_por = usuario
            expediente.total_patronal = preview.total_patronal
            expediente.trabajadores = len(detalles)
            expediente.cruzados = sum(d.empleado_id is not None for d in detalles)
            expediente.sin_cruce = len(detalles) - expediente.cruzados
            expediente.metadata = {
                **(expediente.metadata or {}),
                "sha256_sua": preview.sua.sha256,
                "lineas_actualizadas": resultado.actualizadas,
                "protegidas_manual": resultado.protegidas_manual,
                "conflictos_auto": resultado.conflictos_auto,
                "avisos": list(resultado.avisos),
            }
            expediente.save(update_fields=[
                "estado", "aplicado_en", "aplicado_por", "total_patronal",
                "trabajadores", "cruzados", "sin_cruce", "metadata",
            ])
            documento_ids = list(expediente.documentos.values_list("pk", flat=True))
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
                    "sha256_sua": preview.sua.sha256,
                    "documento_ids": documento_ids,
                    "lineas_actualizadas": resultado.actualizadas,
                    "protegidas_manual": resultado.protegidas_manual,
                    "conflictos_auto": resultado.conflictos_auto,
                },
            )
        return expediente
    except IntegrityError:
        ganador = (
            DocumentoCedulaIMSS.objects.filter(
                sha256=preview.sua.sha256,
                clase=DocumentoCedulaIMSS.CLASE_SUA_XLS,
            )
            .select_related("expediente")
            .first()
        )
        protegidos = set()
        if ganador is not None:
            protegidos = set(
                DocumentoCedulaIMSS.objects.filter(expediente_id=ganador.expediente_id)
                .values_list("archivo", flat=True)
            )
        _limpiar_blobs(blobs_guardados, protegidos=protegidos)
        if ganador is not None:
            return ganador.expediente
        raise
    except Exception:
        _limpiar_blobs(blobs_guardados)
        raise

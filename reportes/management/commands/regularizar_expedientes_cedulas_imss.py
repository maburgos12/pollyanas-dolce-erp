"""Regulariza cédulas SUA históricas sin recalcular sus importes guardados."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import DocumentoCedulaIMSS, LineaPresupuestoMensual
from reportes.services_cedula_expediente import (
    CedulaDiscrepante,
    MAX_ARCHIVO_BYTES,
    _extraer_total_control,
    aplicar_expediente,
    preparar_expediente,
)
from reportes.services_cedula_imss import cargar_filas_xls, parsear_cedula
from reportes.services_presupuesto_maestro import normalize_header_text


FUENTES_HISTORICAS = ("AUTO:LEGADO", "AUTO:SIPARE")


@dataclass(frozen=True)
class ExpedienteHistorico:
    ruta_sua: Path
    rutas_pdf: tuple[Path, ...]
    parseada: object
    sha256_sua: str
    total_patronal: Decimal
    lineas: tuple[LineaPresupuestoMensual, ...]
    monto_existente: Decimal
    preview: object | None = None


def _registro_normalizado(valor: str) -> str:
    return "".join(caracter for caracter in str(valor or "").upper() if caracter.isalnum())


def _enumerar_archivos(root: Path) -> tuple[Path, ...]:
    if not root.exists() or not root.is_dir():
        raise CommandError(f"--root debe ser un directorio existente: {root}")
    if root.is_symlink():
        raise CommandError("--root no puede ser un enlace simbólico.")
    root_real = root.resolve(strict=True)
    archivos: list[Path] = []
    for ruta in sorted(root.rglob("*")):
        if ruta.is_symlink():
            raise CommandError(f"No se permiten enlaces simbólicos bajo --root: {ruta}")
        if not ruta.is_file() or ruta.suffix.lower() not in {".xls", ".pdf"}:
            continue
        resuelta = ruta.resolve(strict=True)
        if not resuelta.is_relative_to(root_real):
            raise CommandError(f"Archivo fuera de --root: {ruta}")
        archivos.append(resuelta)
    if not archivos:
        raise CommandError("No se encontraron archivos .xls o .pdf bajo --root.")
    return tuple(archivos)


def _agrupar_archivos(archivos: tuple[Path, ...]) -> tuple[tuple[Path, tuple[Path, ...]], ...]:
    por_directorio: dict[Path, list[Path]] = {}
    for ruta in archivos:
        por_directorio.setdefault(ruta.parent, []).append(ruta)
    grupos: list[tuple[Path, tuple[Path, ...]]] = []
    for directorio, rutas in sorted(por_directorio.items()):
        xls = sorted(ruta for ruta in rutas if ruta.suffix.lower() == ".xls")
        pdfs = tuple(sorted(ruta for ruta in rutas if ruta.suffix.lower() == ".pdf"))
        if pdfs and len(xls) != 1:
            raise CommandError(
                f"Los PDF de {directorio} son ambiguos: debe haber exactamente un SUA .xls "
                "en el mismo directorio."
            )
        if not xls:
            raise CommandError(f"Hay PDF sin SUA .xls asociado en {directorio}.")
        for ruta_xls in xls:
            grupos.append((ruta_xls, pdfs if len(xls) == 1 else ()))
    return tuple(grupos)


def _subida(ruta: Path) -> SimpleUploadedFile:
    if ruta.stat().st_size > MAX_ARCHIVO_BYTES:
        raise CommandError(f"El archivo '{ruta.name}' excede el límite de 10 MiB.")
    tipo = "application/vnd.ms-excel" if ruta.suffix.lower() == ".xls" else "application/pdf"
    return SimpleUploadedFile(ruta.name, ruta.read_bytes(), content_type=tipo)


def _huella_y_firma(ruta: Path, firma: bytes) -> str:
    if ruta.stat().st_size > MAX_ARCHIVO_BYTES:
        raise CommandError(f"El archivo '{ruta.name}' excede el límite de 10 MiB.")
    digest = hashlib.sha256()
    with ruta.open("rb") as archivo:
        cabecera = archivo.read(len(firma))
        if cabecera != firma:
            raise CommandError(f"El archivo '{ruta.name}' no tiene una firma válida.")
        digest.update(cabecera)
        for bloque in iter(lambda: archivo.read(1024 * 1024), b""):
            digest.update(bloque)
    return digest.hexdigest()


def _validar_pdf_directo(ruta: Path) -> None:
    _huella_y_firma(ruta, b"%PDF-")
    try:
        import pdfplumber
    except ModuleNotFoundError as exc:
        raise CommandError("Dependencia pdfplumber no disponible.") from exc
    try:
        with pdfplumber.open(ruta) as lector:
            if not lector.pages:
                raise ValueError
            texto = normalize_header_text(
                " ".join(pagina.extract_text() or "" for pagina in lector.pages)
            )
    except Exception as exc:
        raise CommandError(f"El archivo '{ruta.name}' no es un PDF válido y parseable.") from exc
    ema = bool(re.search(r"\bema\b", texto)) or "emision mensual anticipada" in texto
    eba = bool(re.search(r"\beba\b", texto)) or "emision bimestral anticipada" in texto
    if ema == eba:
        raise CommandError(f"No se pudo clasificar inequívocamente el PDF '{ruta.name}'.")


def _inspeccionar_sua(ruta: Path):
    sha256 = _huella_y_firma(ruta, b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1")
    try:
        filas = cargar_filas_xls(str(ruta))
        parseada = parsear_cedula(filas)
        total_patronal = _extraer_total_control(filas, parseada.tipo)
    except (OSError, ValueError) as exc:
        raise CommandError(f"No se pudo validar {ruta.name}: {exc}") from exc
    total_detalle = sum(
        (trabajador.patronal for trabajador in parseada.trabajadores), Decimal("0")
    ).quantize(Decimal("0.01"))
    if total_detalle != total_patronal:
        raise CedulaDiscrepante(
            f"{ruta.name}: detalle {total_detalle:.2f} != control patronal {total_patronal:.2f}."
        )
    return parseada, sha256, total_patronal


def _lineas_objetivo(parseada, *, bloquear: bool = False) -> tuple[LineaPresupuestoMensual, ...]:
    conceptos = {"imss"} if parseada.tipo == "MENSUAL" else {"infonavit rcv", "infonavit"}
    queryset = LineaPresupuestoMensual.objects.filter(
        periodo__in=parseada.meses,
        version=LineaPresupuestoMensual.VERSION_ORIGINAL,
    )
    if bloquear:
        queryset = queryset.select_for_update()
    candidatas = queryset.select_related("rubro__area").order_by("pk")
    return tuple(
        linea for linea in candidatas if normalize_header_text(linea.rubro.concepto) in conceptos
    )


def _lineas_materializadas(parseada) -> tuple[LineaPresupuestoMensual, ...]:
    candidatas = (
        linea
        for linea in _lineas_objetivo(parseada)
        if linea.fuente_real in FUENTES_HISTORICAS and linea.monto_real is not None
    )
    return tuple(candidatas)


def _monto_control(parseada, lineas: tuple[LineaPresupuestoMensual, ...]) -> Decimal:
    control = [linea for linea in lineas if linea.rubro.area.codigo == "nomina"]
    periodos_control = {linea.periodo for linea in control}
    if periodos_control != set(parseada.meses):
        faltantes = ", ".join(
            mes.isoformat() for mes in parseada.meses if mes not in periodos_control
        )
        raise CommandError(
            f"No existe materialización histórica completa de Nómina para {faltantes or 'el periodo'}."
        )
    return sum((linea.monto_real for linea in control), Decimal("0")).quantize(Decimal("0.01"))


def _verificar_sha_materializado(sha256_sua, lineas: tuple[LineaPresupuestoMensual, ...]) -> None:
    for linea in lineas:
        documento = (linea.metadata or {}).get("cedula_imss_documento", {})
        sha_historico = documento.get("sha256") if isinstance(documento, dict) else None
        if sha_historico and sha_historico != sha256_sua:
            raise CommandError(
                f"La línea histórica {linea.pk} referencia una huella SHA-256 distinta."
            )


class Command(BaseCommand):
    help = "Regulariza expedientes históricos de cédulas IMSS (dry-run por defecto)."

    def add_arguments(self, parser):
        parser.add_argument("--root", required=True)
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--registro", default="E52-40157-10-0")

    def handle(self, *args, **options):
        root = Path(options["root"]).expanduser()
        grupos = _agrupar_archivos(_enumerar_archivos(root))
        registro_esperado = _registro_normalizado(options["registro"])
        if not registro_esperado:
            raise CommandError("--registro debe contener un registro patronal válido.")
        preparados: list[ExpedienteHistorico] = []

        for ruta_sua, pdfs in grupos:
            parseada, sha256_sua, total_patronal = _inspeccionar_sua(ruta_sua)
            for ruta_pdf in pdfs:
                _validar_pdf_directo(ruta_pdf)
            if _registro_normalizado(parseada.registro_patronal) != registro_esperado:
                raise CommandError(
                    f"Registro patronal inesperado en {ruta_sua.name}: "
                    f"{parseada.registro_patronal}."
                )
            lineas = _lineas_materializadas(parseada)
            _verificar_sha_materializado(sha256_sua, lineas)
            monto_existente = _monto_control(parseada, lineas)
            if monto_existente != total_patronal:
                raise CommandError(
                    f"{ruta_sua.name}: total documento {total_patronal:.2f} no coincide "
                    f"con monto existente {monto_existente:.2f}."
                )
            preview = None
            if options["apply"]:
                try:
                    archivos = [_subida(ruta_sua), *(_subida(ruta) for ruta in pdfs)]
                    preview = preparar_expediente(archivos, usuario=None)
                except Exception as exc:
                    raise CommandError(f"No se pudo preparar {ruta_sua.name}: {exc}") from exc
                if (
                    preview.sua.sha256 != sha256_sua
                    or preview.parseada.tipo != parseada.tipo
                    or preview.parseada.periodo != parseada.periodo
                    or preview.total_patronal != total_patronal
                ):
                    raise CommandError(
                        f"{ruta_sua.name}: la inspección directa y la preparación no coinciden."
                    )
            preparados.append(
                ExpedienteHistorico(
                    ruta_sua=ruta_sua,
                    rutas_pdf=pdfs,
                    parseada=parseada,
                    sha256_sua=sha256_sua,
                    total_patronal=total_patronal,
                    lineas=lineas,
                    monto_existente=monto_existente,
                    preview=preview,
                )
            )

        encabezado = "periodo | archivo | total_documento | monto_existente | accion"
        filas_salida: list[str] = []
        if not options["apply"]:
            self.stdout.write(encabezado)
            for item in preparados:
                filas_salida.append(
                    f"{item.parseada.periodo:%Y-%m} | {item.ruta_sua.name} | "
                    f"{item.total_patronal:.2f} | {item.monto_existente:.2f} | DRY-RUN"
                )
            for fila in filas_salida:
                self.stdout.write(fila)
            return

        blobs_nuevos: list[tuple[object, str]] = []
        try:
            with transaction.atomic():
                for item in preparados:
                    if item.preview is None:
                        raise CommandError("Falta la preparación transaccional del expediente.")
                    lineas_preexistentes = _lineas_objetivo(item.parseada, bloquear=True)
                    ids_preexistentes = {linea.pk for linea in lineas_preexistentes}
                    lineas_bloqueadas = tuple(
                        linea
                        for linea in lineas_preexistentes
                        if linea.fuente_real in FUENTES_HISTORICAS
                        and linea.monto_real is not None
                    )
                    if {linea.pk for linea in lineas_bloqueadas} != {
                        linea.pk for linea in item.lineas
                    }:
                        raise CommandError("La materialización histórica cambió durante la ejecución.")
                    _verificar_sha_materializado(item.sha256_sua, lineas_bloqueadas)
                    if _monto_control(item.parseada, lineas_bloqueadas) != item.monto_existente:
                        raise CommandError("El monto histórico cambió durante la ejecución.")
                    instantaneas = {
                        linea.pk: (
                            linea.monto_presupuesto,
                            linea.monto_real,
                            linea.fuente_real,
                            dict(linea.metadata or {}),
                            linea.actualizado_en,
                        )
                        for linea in lineas_preexistentes
                    }
                    for linea in lineas_bloqueadas:
                        vinculo = (linea.metadata or {}).get("expediente_cedula_imss_id")
                        documento_vinculado = (linea.metadata or {}).get(
                            "documento_cedula_imss_id"
                        )
                        if vinculo and documento_vinculado:
                            documento = DocumentoCedulaIMSS.objects.filter(
                                pk=documento_vinculado,
                                expediente_id=vinculo,
                                sha256=item.sha256_sua,
                            ).first()
                            if documento is None:
                                raise CommandError(
                                    f"La línea histórica {linea.pk} tiene un enlace documental "
                                    "inconsistente."
                                )
                    shas_previos = set(
                        DocumentoCedulaIMSS.objects.filter(
                            sha256__in=[d.sha256 for d in item.preview.documentos]
                        ).values_list("sha256", flat=True)
                    )
                    expediente = aplicar_expediente(item.preview, usuario=None)
                    documento_sua = expediente.documentos.get(
                        clase=DocumentoCedulaIMSS.CLASE_SUA_XLS
                    )
                    for documento in expediente.documentos.exclude(sha256__in=shas_previos):
                        blobs_nuevos.append((documento.archivo.storage, documento.archivo.name))

                    actuales = _lineas_objetivo(item.parseada)
                    LineaPresupuestoMensual.objects.filter(
                        pk__in=[linea.pk for linea in actuales if linea.pk not in ids_preexistentes]
                    ).delete()
                    por_id = {linea.pk: linea for linea in lineas_preexistentes}
                    ids_historicos = {linea.pk for linea in lineas_bloqueadas}
                    for linea_id, instantanea in instantaneas.items():
                        presupuesto, monto, fuente, metadata_original, actualizado_en = instantanea
                        metadata = metadata_original
                        if linea_id in ids_historicos:
                            vinculo = metadata.get("expediente_cedula_imss_id")
                            if vinculo not in (None, expediente.pk):
                                raise CommandError(
                                    f"La línea histórica {linea_id} ya pertenece al expediente "
                                    f"{vinculo}."
                                )
                            metadata["expediente_cedula_imss_id"] = expediente.pk
                            metadata["documento_cedula_imss_id"] = documento_sua.pk
                            metadata["cedula_imss"] = {
                                "tipo": item.parseada.tipo,
                                "registro_patronal": item.parseada.registro_patronal,
                            }
                            metadata["cedula_imss_documento"] = {
                                "archivo": item.ruta_sua.name,
                                "sha256": item.sha256_sua,
                                "registro_patronal": item.parseada.registro_patronal,
                            }
                        linea = por_id[linea_id]
                        linea.monto_presupuesto = presupuesto
                        linea.monto_real = monto
                        linea.fuente_real = fuente
                        linea.metadata = metadata
                        linea.actualizado_en = actualizado_en
                    LineaPresupuestoMensual.objects.bulk_update(
                        tuple(por_id.values()),
                        [
                            "monto_presupuesto",
                            "monto_real",
                            "fuente_real",
                            "metadata",
                            "actualizado_en",
                        ],
                    )
                    accion = "YA_ENLAZADO" if item.sha256_sua in shas_previos else "APLICADO"
                    filas_salida.append(
                        f"{item.parseada.periodo:%Y-%m} | {item.ruta_sua.name} | "
                        f"{item.total_patronal:.2f} | {item.monto_existente:.2f} | {accion}"
                    )
        except Exception:
            for storage, nombre in reversed(blobs_nuevos):
                storage.delete(nombre)
            raise

        self.stdout.write(encabezado)
        for fila in filas_salida:
            self.stdout.write(fila)

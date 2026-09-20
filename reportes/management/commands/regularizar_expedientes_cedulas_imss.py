"""Regulariza cédulas SUA históricas sin recalcular sus importes guardados."""

from __future__ import annotations

import hashlib
import io
import logging
import os
import re
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models.signals import post_save

from reportes.models import DocumentoCedulaIMSS, LineaPresupuestoMensual
from reportes.services_cedula_expediente import (
    CedulaDiscrepante,
    MAX_ARCHIVO_BYTES,
    MAX_EXPEDIENTE_BYTES,
    _extraer_total_control,
    _bloquear_familia,
    aplicar_expediente,
    preparar_expediente,
)
from reportes.services_cedula_imss import parsear_cedula
from reportes.services_presupuesto_maestro import normalize_header_text


FUENTES_HISTORICAS = ("AUTO:LEGADO", "AUTO:SIPARE")
logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class ArchivoInspeccionado:
    ruta: Path
    sha256: str
    tamano: int
    clase: str
    registro_patronal: str
    periodo: object
    parseada: object | None = None
    total_patronal: Decimal | None = None


@dataclass(frozen=True)
class IdentidadCedula:
    tipo: str
    periodo: date
    registro_patronal: str

    @property
    def meses(self) -> tuple[date, ...]:
        if self.tipo == "BIMESTRAL":
            return (self.periodo.replace(month=self.periodo.month - 1), self.periodo)
        return (self.periodo,)


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


def _leer_archivo_seguro(ruta: Path, root: Path) -> tuple[bytes, str]:
    root_lexico = Path(os.path.abspath(root))
    try:
        root_absoluto = root.resolve(strict=True)
    except OSError as exc:
        raise CommandError(f"No se pudo resolver --root de forma segura: {exc}") from exc
    ruta_absoluta = Path(os.path.abspath(ruta))
    try:
        relativa = ruta_absoluta.relative_to(root_lexico)
    except ValueError as exc:
        try:
            relativa = ruta_absoluta.relative_to(root_absoluto)
        except ValueError:
            raise CommandError(f"Archivo fuera de --root: {ruta}") from exc
    componentes = relativa.parts
    if not componentes or any(parte in {"", ".", ".."} for parte in componentes):
        raise CommandError(f"Ruta inválida bajo --root: {ruta}")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    directorio_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | nofollow
    archivo_flags = os.O_RDONLY | nofollow
    directorios_abiertos: list[int] = []
    archivo_fd = None
    try:
        actual = os.open(root_absoluto, directorio_flags)
        directorios_abiertos.append(actual)
        for componente in componentes[:-1]:
            actual = os.open(componente, directorio_flags, dir_fd=actual)
            directorios_abiertos.append(actual)
        archivo_fd = os.open(componentes[-1], archivo_flags, dir_fd=actual)
    except OSError as exc:
        for directorio_fd in reversed(directorios_abiertos):
            os.close(directorio_fd)
        raise CommandError(f"No se pudo abrir de forma segura '{ruta.name}': {exc}") from exc
    try:
        estado = os.fstat(archivo_fd)
        if not stat.S_ISREG(estado.st_mode):
            raise CommandError(f"'{ruta.name}' no es un archivo regular.")
        if estado.st_size > MAX_ARCHIVO_BYTES:
            raise CommandError(f"El archivo '{ruta.name}' excede el límite de 10 MiB.")
        partes, total = [], 0
        while True:
            bloque = os.read(
                archivo_fd,
                min(1024 * 1024, MAX_ARCHIVO_BYTES + 1 - total),
            )
            if not bloque:
                break
            partes.append(bloque)
            total += len(bloque)
            if total > MAX_ARCHIVO_BYTES:
                raise CommandError(f"El archivo '{ruta.name}' excede el límite de 10 MiB.")
        contenido = b"".join(partes)
        if total != estado.st_size:
            raise CommandError(f"El archivo '{ruta.name}' cambió durante la lectura.")
        return contenido, hashlib.sha256(contenido).hexdigest()
    finally:
        if archivo_fd is not None:
            os.close(archivo_fd)
        for directorio_fd in reversed(directorios_abiertos):
            os.close(directorio_fd)


def _cargar_filas_bytes(contenido: bytes):
    import xlrd
    libro = xlrd.open_workbook(file_contents=contenido)
    hoja = libro.sheet_by_index(0)
    return [[hoja.cell_value(r, c) for c in range(hoja.ncols)] for r in range(hoja.nrows)]


def _inspeccionar_pdf(ruta: Path, contenido: bytes, sha256: str) -> ArchivoInspeccionado:
    if not contenido.startswith(b"%PDF-"):
        raise CommandError(f"El archivo '{ruta.name}' no tiene una firma válida.")
    try:
        import pdfplumber
    except ModuleNotFoundError as exc:
        raise CommandError("Dependencia pdfplumber no disponible.") from exc
    try:
        with pdfplumber.open(io.BytesIO(contenido)) as lector:
            if not lector.pages:
                raise ValueError
            texto_original = " ".join(pagina.extract_text() or "" for pagina in lector.pages)
            texto = normalize_header_text(texto_original)
    except Exception as exc:
        raise CommandError(f"El archivo '{ruta.name}' no es un PDF válido y parseable.") from exc
    texto_compacto = texto.replace(" ", "")
    ema = (
        bool(re.search(r"\bema\b", texto))
        or "emision mensual anticipada" in texto
        or ("periodo" in texto and "cuotasenfermedadesymaternidad" in texto_compacto)
    )
    eba = (
        bool(re.search(r"\beba\b", texto))
        or "emision bimestral anticipada" in texto
        or ("bimestre" in texto and "cuotasrcv" in texto_compacto)
    )
    if ema == eba:
        raise CommandError(f"No se pudo clasificar inequívocamente el PDF '{ruta.name}'.")
    registros = re.findall(r"[A-Z]\d{2}-\d{5}-\d{2}-\d", texto_original.upper())
    patron_periodo = r"PERIODO.{0,200}?(\d{2})-(\d{4})" if ema else r"BIMESTRE.{0,200}?(\d{2})-(\d{4})"
    match = re.search(patron_periodo, texto_original.upper(), re.DOTALL)
    if len(set(registros)) != 1 or match is None:
        raise CommandError(f"PDF '{ruta.name}' sin registro/periodo inequívoco.")
    numero, anio = int(match.group(1)), int(match.group(2))
    mes = numero if ema else numero * 2
    if not 1 <= mes <= 12:
        raise CommandError(f"Periodo inválido en PDF '{ruta.name}'.")
    return ArchivoInspeccionado(
        ruta, sha256, len(contenido), "EMA_PDF" if ema else "EBA_PDF",
        registros[0], date(anio, mes, 1),
    )


def _inspeccionar_sua(ruta: Path, contenido: bytes, sha256: str):
    if not contenido.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        raise CommandError(f"El archivo '{ruta.name}' no tiene una firma válida.")
    try:
        filas = _cargar_filas_bytes(contenido)
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
    identidad = IdentidadCedula(parseada.tipo, parseada.periodo, parseada.registro_patronal)
    return ArchivoInspeccionado(
        ruta, sha256, len(contenido), "SUA_XLS", parseada.registro_patronal,
        parseada.periodo, identidad, total_patronal,
    )


def _agrupar_inspecciones(
    documentos: tuple[ArchivoInspeccionado, ...],
) -> tuple[tuple[ArchivoInspeccionado, tuple[ArchivoInspeccionado, ...]], ...]:
    suas = [d for d in documentos if d.clase == "SUA_XLS"]
    pdfs = [d for d in documentos if d.clase != "SUA_XLS"]
    asociados: dict[str, list[ArchivoInspeccionado]] = {s.sha256: [] for s in suas}
    for pdf in pdfs:
        tipo = "MENSUAL" if pdf.clase == "EMA_PDF" else "BIMESTRAL"
        candidatos = [
            sua for sua in suas
            if sua.parseada.tipo == tipo
            and sua.periodo == pdf.periodo
            and _registro_normalizado(sua.registro_patronal)
            == _registro_normalizado(pdf.registro_patronal)
        ]
        if len(candidatos) != 1:
            raise CommandError(
                f"PDF '{pdf.ruta.name}' no tiene un SUA único con tipo, registro y periodo coincidentes."
            )
        asociados[candidatos[0].sha256].append(pdf)
    grupos = []
    for sua in sorted(suas, key=lambda d: str(d.ruta)):
        evidencias = tuple(asociados[sua.sha256])
        if sua.tamano + sum(p.tamano for p in evidencias) > MAX_EXPEDIENTE_BYTES:
            raise CommandError(f"El expediente de '{sua.ruta.name}' excede 30 MiB.")
        grupos.append((sua, evidencias))
    return tuple(grupos)


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


@contextmanager
def _capturar_blobs_creados(
    destino: list[tuple[object, str]],
    *,
    shas_esperados: set[str],
    nombres_protegidos: set[str],
):
    """Registra blobs al guardar Documento, antes de cualquier consulta posterior."""
    hilo = threading.get_ident()
    dispatch_uid = f"regularizar-cedulas-imss-{id(destino)}-{hilo}"

    def registrar(sender, instance, created, **kwargs):
        if (
            created
            and threading.get_ident() == hilo
            and instance.sha256 in shas_esperados
            and instance.archivo.name
            and instance.archivo.name not in nombres_protegidos
        ):
            destino.append((instance.archivo.storage, instance.archivo.name))

    post_save.connect(
        registrar,
        sender=DocumentoCedulaIMSS,
        dispatch_uid=dispatch_uid,
        weak=False,
    )
    try:
        yield
    finally:
        post_save.disconnect(
            sender=DocumentoCedulaIMSS,
            dispatch_uid=dispatch_uid,
        )


def _limpiar_blobs(blobs: list[tuple[object, str]]) -> None:
    for storage, nombre in reversed(blobs):
        try:
            storage.delete(nombre)
        except Exception:
            logger.warning("No se pudo limpiar blob de backfill IMSS: %s", nombre, exc_info=True)


class Command(BaseCommand):
    help = "Regulariza expedientes históricos de cédulas IMSS (dry-run por defecto)."

    def add_arguments(self, parser):
        parser.add_argument("--root", required=True)
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--registro", default="E52-40157-10-0")

    def handle(self, *args, **options):
        root = Path(options["root"]).expanduser()
        rutas = _enumerar_archivos(root)
        inspecciones = []
        for ruta in rutas:
            contenido, sha256 = _leer_archivo_seguro(ruta, root)
            if ruta.suffix.lower() == ".xls":
                inspecciones.append(_inspeccionar_sua(ruta, contenido, sha256))
            else:
                inspecciones.append(_inspeccionar_pdf(ruta, contenido, sha256))
            del contenido
        grupos = _agrupar_inspecciones(tuple(inspecciones))
        registro_esperado = _registro_normalizado(options["registro"])
        if not registro_esperado:
            raise CommandError("--registro debe contener un registro patronal válido.")
        preparados: list[ExpedienteHistorico] = []

        for sua, pdfs in grupos:
            ruta_sua = sua.ruta
            parseada, sha256_sua, total_patronal = sua.parseada, sua.sha256, sua.total_patronal
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
            preparados.append(
                ExpedienteHistorico(
                    ruta_sua=ruta_sua,
                    rutas_pdf=tuple(pdf.ruta for pdf in pdfs),
                    parseada=parseada,
                    sha256_sua=sha256_sua,
                    total_patronal=total_patronal,
                    lineas=lineas,
                    monto_existente=monto_existente,
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
                    lecturas = []
                    for ruta, sha_esperado in [
                        (item.ruta_sua, item.sha256_sua),
                        *[(pdf.ruta, pdf.sha256) for sua, pdfs in grupos if sua.sha256 == item.sha256_sua for pdf in pdfs],
                    ]:
                        contenido, sha_actual = _leer_archivo_seguro(ruta, root)
                        if sha_actual != sha_esperado:
                            raise CommandError(f"'{ruta.name}' cambió después del preflight.")
                        lecturas.append((ruta, contenido))
                    archivos = [
                        SimpleUploadedFile(
                            ruta.name,
                            contenido,
                            content_type=(
                                "application/vnd.ms-excel"
                                if ruta.suffix.lower() == ".xls"
                                else "application/pdf"
                            ),
                        )
                        for ruta, contenido in lecturas
                    ]
                    try:
                        preview = preparar_expediente(archivos, usuario=None)
                    except Exception as exc:
                        raise CommandError(f"No se pudo preparar {item.ruta_sua.name}: {exc}") from exc
                    if (
                        preview.sua.sha256 != item.sha256_sua
                        or preview.parseada.tipo != item.parseada.tipo
                        or preview.parseada.periodo != item.parseada.periodo
                        or preview.total_patronal != item.total_patronal
                    ):
                        raise CommandError(
                            f"{item.ruta_sua.name}: preflight y preparación no coinciden."
                        )
                    _bloquear_familia(preview)
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
                            sha256__in=[d.sha256 for d in preview.documentos]
                        ).values_list("sha256", flat=True)
                    )
                    nombres_protegidos = set(
                        DocumentoCedulaIMSS.objects.filter(sha256__in=shas_previos).values_list(
                            "archivo", flat=True
                        )
                    )
                    with _capturar_blobs_creados(
                        blobs_nuevos,
                        shas_esperados={d.sha256 for d in preview.documentos},
                        nombres_protegidos=nombres_protegidos,
                    ):
                        expediente = aplicar_expediente(preview, usuario=None)
                    documento_sua = expediente.documentos.get(
                        clase=DocumentoCedulaIMSS.CLASE_SUA_XLS
                    )

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
                    del archivos, contenido, lecturas, preview
        except Exception:
            _limpiar_blobs(blobs_nuevos)
            raise

        self.stdout.write(encabezado)
        for fila in filas_salida:
            self.stdout.write(fila)

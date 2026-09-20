"""Regulariza cédulas SUA históricas sin recalcular sus importes guardados."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import DocumentoCedulaIMSS, LineaPresupuestoMensual
from reportes.services_cedula_expediente import (
    MAX_ARCHIVO_BYTES,
    aplicar_expediente,
    preparar_expediente,
)
from reportes.services_presupuesto_maestro import normalize_header_text


FUENTES_HISTORICAS = ("AUTO:LEGADO", "AUTO:SIPARE")


@dataclass(frozen=True)
class ExpedienteHistorico:
    ruta_sua: Path
    preview: object
    lineas: tuple[LineaPresupuestoMensual, ...]
    monto_existente: Decimal


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


def _lineas_materializadas(preview) -> tuple[LineaPresupuestoMensual, ...]:
    conceptos = {"imss"} if preview.parseada.tipo == "MENSUAL" else {"infonavit rcv", "infonavit"}
    candidatas = (
        LineaPresupuestoMensual.objects.filter(
            periodo__in=preview.parseada.meses,
            version=LineaPresupuestoMensual.VERSION_ORIGINAL,
            fuente_real__in=FUENTES_HISTORICAS,
            monto_real__isnull=False,
        )
        .select_related("rubro__area")
        .order_by("pk")
    )
    return tuple(
        linea
        for linea in candidatas
        if normalize_header_text(linea.rubro.concepto) in conceptos
    )


def _monto_control(preview, lineas: tuple[LineaPresupuestoMensual, ...]) -> Decimal:
    control = [linea for linea in lineas if linea.rubro.area.codigo == "nomina"]
    periodos_control = {linea.periodo for linea in control}
    if periodos_control != set(preview.parseada.meses):
        faltantes = ", ".join(
            mes.isoformat() for mes in preview.parseada.meses if mes not in periodos_control
        )
        raise CommandError(
            f"No existe materialización histórica completa de Nómina para {faltantes or 'el periodo'}."
        )
    return sum((linea.monto_real for linea in control), Decimal("0")).quantize(Decimal("0.01"))


def _verificar_sha_materializado(preview, lineas: tuple[LineaPresupuestoMensual, ...]) -> None:
    for linea in lineas:
        documento = (linea.metadata or {}).get("cedula_imss_documento", {})
        sha_historico = documento.get("sha256") if isinstance(documento, dict) else None
        if sha_historico and sha_historico != preview.sua.sha256:
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
            try:
                archivos = [_subida(ruta_sua), *(_subida(ruta) for ruta in pdfs)]
                preview = preparar_expediente(archivos, usuario=None)
            except Exception as exc:
                raise CommandError(f"No se pudo validar {ruta_sua.name}: {exc}") from exc
            sha_archivo = hashlib.sha256(ruta_sua.read_bytes()).hexdigest()
            if sha_archivo != preview.sua.sha256:
                raise CommandError(f"La huella SHA-256 cambió durante la lectura de {ruta_sua.name}.")
            if _registro_normalizado(preview.parseada.registro_patronal) != registro_esperado:
                raise CommandError(
                    f"Registro patronal inesperado en {ruta_sua.name}: "
                    f"{preview.parseada.registro_patronal}."
                )
            lineas = _lineas_materializadas(preview)
            _verificar_sha_materializado(preview, lineas)
            monto_existente = _monto_control(preview, lineas)
            if monto_existente != preview.total_patronal:
                raise CommandError(
                    f"{ruta_sua.name}: total documento {preview.total_patronal:.2f} no coincide "
                    f"con monto existente {monto_existente:.2f}."
                )
            preparados.append(ExpedienteHistorico(ruta_sua, preview, lineas, monto_existente))

        encabezado = "periodo | archivo | total_documento | monto_existente | accion"
        filas_salida: list[str] = []
        if not options["apply"]:
            self.stdout.write(encabezado)
            for item in preparados:
                filas_salida.append(
                    f"{item.preview.parseada.periodo:%Y-%m} | {item.ruta_sua.name} | "
                    f"{item.preview.total_patronal:.2f} | {item.monto_existente:.2f} | DRY-RUN"
                )
            for fila in filas_salida:
                self.stdout.write(fila)
            return

        blobs_nuevos: list[tuple[object, str]] = []
        try:
            with transaction.atomic():
                for item in preparados:
                    ids_originales = {linea.pk for linea in item.lineas}
                    lineas_bloqueadas = tuple(
                        LineaPresupuestoMensual.objects.select_for_update()
                        .filter(pk__in=ids_originales)
                        .select_related("rubro__area")
                        .order_by("pk")
                    )
                    if len(lineas_bloqueadas) != len(ids_originales):
                        raise CommandError("La materialización histórica cambió durante la ejecución.")
                    _verificar_sha_materializado(item.preview, lineas_bloqueadas)
                    if _monto_control(item.preview, lineas_bloqueadas) != item.monto_existente:
                        raise CommandError("El monto histórico cambió durante la ejecución.")
                    instantaneas = {
                        linea.pk: (linea.monto_real, linea.fuente_real, dict(linea.metadata or {}))
                        for linea in lineas_bloqueadas
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
                                sha256=item.preview.sua.sha256,
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

                    actuales = _lineas_materializadas(item.preview)
                    LineaPresupuestoMensual.objects.filter(
                        pk__in=[linea.pk for linea in actuales if linea.pk not in ids_originales]
                    ).delete()
                    por_id = {linea.pk: linea for linea in lineas_bloqueadas}
                    for linea_id, (monto, fuente, metadata_original) in instantaneas.items():
                        metadata = metadata_original
                        vinculo = metadata.get("expediente_cedula_imss_id")
                        if vinculo not in (None, expediente.pk):
                            raise CommandError(
                                f"La línea histórica {linea_id} ya pertenece al expediente {vinculo}."
                            )
                        metadata["expediente_cedula_imss_id"] = expediente.pk
                        metadata["documento_cedula_imss_id"] = documento_sua.pk
                        metadata["cedula_imss"] = {
                            "tipo": item.preview.parseada.tipo,
                            "registro_patronal": item.preview.parseada.registro_patronal,
                        }
                        metadata["cedula_imss_documento"] = {
                            "archivo": item.ruta_sua.name,
                            "sha256": item.preview.sua.sha256,
                            "registro_patronal": item.preview.parseada.registro_patronal,
                        }
                        linea = por_id[linea_id]
                        linea.monto_real = monto
                        linea.fuente_real = fuente
                        linea.metadata = metadata
                    LineaPresupuestoMensual.objects.bulk_update(
                        tuple(por_id.values()), ["monto_real", "fuente_real", "metadata"]
                    )
                    accion = "YA_ENLAZADO" if item.preview.sua.sha256 in shas_previos else "APLICADO"
                    filas_salida.append(
                        f"{item.preview.parseada.periodo:%Y-%m} | {item.ruta_sua.name} | "
                        f"{item.preview.total_patronal:.2f} | {item.monto_existente:.2f} | {accion}"
                    )
        except Exception:
            for storage, nombre in reversed(blobs_nuevos):
                storage.delete(nombre)
            raise

        self.stdout.write(encabezado)
        for fila in filas_salida:
            self.stdout.write(fila)

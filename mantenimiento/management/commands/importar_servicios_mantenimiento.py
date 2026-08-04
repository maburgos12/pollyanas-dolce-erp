import csv
from collections import OrderedDict
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils.dateparse import parse_date

from activos.models import Activo
from core.models import Sucursal
from logistica.models import Unidad
from maestros.models import Proveedor
from mantenimiento.models import ServicioMantenimiento
from mantenimiento.services_grouped import create_grouped_service


REQUIRED_COLUMNS = {
    "grupo_id", "fecha_servicio", "descripcion_general", "costo_total",
    "tipo_objetivo", "trabajo_realizado",
}


class Command(BaseCommand):
    help = "Valida o importa servicios agrupados de mantenimiento desde CSV. Dry-run por defecto."

    def add_arguments(self, parser):
        parser.add_argument("archivo")
        parser.add_argument("--usuario", required=True, help="Usuario ERP que quedará como creador.")
        parser.add_argument("--apply", action="store_true", help="Aplica la carga después de validar.")
        parser.add_argument("--confirmar", default="", help="Debe ser CARGAR_MANTENIMIENTO junto con --apply.")

    def handle(self, *args, **options):
        path = Path(options["archivo"]).expanduser().resolve()
        if not path.is_file() or path.suffix.lower() != ".csv":
            raise CommandError("Indica un archivo CSV existente.")
        if options["apply"] and options["confirmar"] != "CARGAR_MANTENIMIENTO":
            raise CommandError("Para aplicar usa --apply --confirmar CARGAR_MANTENIMIENTO.")
        user = get_user_model().objects.filter(username=options["usuario"], is_active=True).first()
        if user is None:
            raise CommandError("El usuario ERP no existe o está inactivo.")

        with path.open(newline="", encoding="utf-8-sig") as source:
            reader = csv.DictReader(source)
            missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
            if missing:
                raise CommandError(f"Faltan columnas: {', '.join(sorted(missing))}.")
            groups = OrderedDict()
            for line_number, row in enumerate(reader, start=2):
                group_id = (row.get("grupo_id") or "").strip()
                if not group_id:
                    raise CommandError(f"Fila {line_number}: grupo_id vacío.")
                groups.setdefault(group_id, []).append((line_number, row))

        results = []
        errors = []
        with transaction.atomic():
            for group_id, group_rows in groups.items():
                try:
                    result = self._process_group(group_id, group_rows, user, apply=options["apply"])
                    results.append(result)
                except (ValidationError, CommandError) as exc:
                    messages = exc.messages if isinstance(exc, ValidationError) else [str(exc)]
                    errors.append(f"{group_id}: {'; '.join(messages)}")
            if errors:
                transaction.set_rollback(True)

        for result in results:
            self.stdout.write(
                f"{result['status']}: {result['group_id']} · {result['details']} alcance(s) · ${result['total']:.2f}"
            )
        if errors:
            for error in errors:
                self.stderr.write(f"ERROR: {error}")
            raise CommandError(f"Validación fallida: {len(errors)} grupo(s) con errores; no se completó la carga.")

        mode = "APLICADA" if options["apply"] else "SIMULADA"
        self.stdout.write(self.style.SUCCESS(f"CARGA {mode}: {len(results)} documento(s), sin duplicar facturas."))

    def _process_group(self, group_id, rows, user, *, apply):
        first = rows[0][1]
        date = parse_date((first.get("fecha_servicio") or "").strip())
        if date is None:
            raise CommandError("fecha_servicio debe usar YYYY-MM-DD.")
        details = []
        for line_number, row in rows:
            kind = (row.get("tipo_objetivo") or "").strip().upper()
            detail = {
                "tipo_objetivo": kind,
                "trabajo_realizado": (row.get("trabajo_realizado") or "").strip(),
                "instalacion_categoria": (row.get("instalacion_categoria") or "").strip(),
                "ubicacion": (row.get("ubicacion") or "").strip(),
                "costo_asignado": (row.get("costo_asignado") or "").strip() or None,
                "costo_estimado": (first.get("metodo_distribucion") or "").strip().upper() == "PRORRATEO",
            }
            if kind == "ACTIVO":
                code = (row.get("codigo_activo") or "").strip()
                obj = Activo.objects.filter(codigo=code, activo=True).first()
                if obj is None:
                    raise CommandError(f"fila {line_number}: activo {code or '(vacío)'} no encontrado.")
                detail["activo_id"] = obj.pk
            elif kind == "UNIDAD":
                code = (row.get("codigo_unidad") or "").strip()
                obj = Unidad.objects.filter(codigo=code, activa=True).first()
                if obj is None:
                    raise CommandError(f"fila {line_number}: unidad {code or '(vacía)'} no encontrada.")
                detail["unidad_id"] = obj.pk
            elif kind == "INSTALACION":
                code = (row.get("codigo_sucursal") or "").strip()
                obj = Sucursal.objects.filter(codigo=code, activa=True).first()
                if obj is None:
                    raise CommandError(f"fila {line_number}: sucursal {code or '(vacía)'} no encontrada.")
                detail["sucursal_id"] = obj.pk
            details.append(detail)

        payload = {
            "fecha_servicio": date,
            "proveedor_nombre": (first.get("proveedor") or "").strip(),
            "responsable": (first.get("responsable") or "").strip(),
            "numero_documento": (first.get("numero_documento") or "").strip(),
            "descripcion_general": (first.get("descripcion_general") or "").strip(),
            "costo_total": (first.get("costo_total") or "").strip(),
            "metodo_distribucion": (first.get("metodo_distribucion") or "SIN_DESGLOSE").strip().upper(),
            "clave_origen": f"BITACORA:{group_id}",
        }
        provider_name = payload["proveedor_nombre"]
        provider_id = (first.get("proveedor_id") or "").strip()
        if provider_name and not provider_id:
            raise CommandError(
                f"proveedor {provider_name}: indica proveedor_id del catálogo ERP; la importación no crea proveedores."
            )
        if provider_id:
            provider = Proveedor.objects.filter(pk=provider_id, activo=True).first()
            if provider is None:
                raise CommandError(f"proveedor_id {provider_id} no existe o está inactivo.")
            payload["proveedor_id"] = provider.pk
        charge_code = (first.get("codigo_sucursal_cargo") or "").strip()
        if charge_code:
            charge_branch = Sucursal.objects.filter(codigo=charge_code, activa=True).first()
            if charge_branch is None:
                raise CommandError(f"centro de costo {charge_code} no encontrado.")
            payload["sucursal_cargo_id"] = charge_branch.pk
        with transaction.atomic():
            service, created = create_grouped_service(payload=payload, details=details, user=user)
            if not apply:
                transaction.set_rollback(True)
        return {
            "group_id": group_id,
            "status": "CREAR" if created else "YA_EXISTE",
            "details": len(details),
            "total": service.costo_total,
        }

import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path
from uuid import UUID, uuid5

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from core.models import Sucursal
from inventario.conteos_access import puede_coordinar
from inventario.models_conteos import (
    ConteoSucursal,
    EventoConteoSucursal,
    LecturaConteoSucursal,
    LineaConteoSucursal,
)
from inventario.services_conteos import ConteoError, _cantidad, resolver_articulo_conteo
from maestros.models import Insumo
from pos_bridge.models import PointProduct


IMPORT_NAMESPACE = UUID("2dd92c5d-e05f-4bb0-a984-79af51b4949b")


def _text(value, field, limit=255):
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > limit:
        raise CommandError(f"{field} está blanco, ausente o es demasiado largo.")
    return value.strip()


def _fingerprint(payload):
    try:
        raw = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    except (TypeError, ValueError) as exc:
        raise CommandError("El payload JSON contiene valores inválidos.") from exc
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _resolver_linea(raw, *, bloquear=False):
    if not isinstance(raw, dict):
        raise CommandError("Cada conteo debe ser un objeto JSON.")
    domain = raw.get("domain")
    allowed = {
        "producto": {
            "domain", "catalogo_id", "sku", "catalogo_nombre", "external_id",
            "cantidad", "hoja", "celda",
        },
        "insumo": {
            "domain", "catalogo_id", "codigo_point", "catalogo_nombre",
            "cantidad", "hoja", "celda",
        },
    }
    if domain not in allowed or set(raw) - allowed[domain]:
        raise CommandError("Dominio o campos de conteo inválidos.")
    ident = raw.get("catalogo_id")
    if type(ident) is not int or ident <= 0:
        raise CommandError("catalogo_id es obligatorio; resolver solo por código es ambiguo.")
    code_field = "sku" if domain == "producto" else "codigo_point"
    code = _text(raw.get(code_field), code_field, 120 if domain == "producto" else 80)
    expected_name = _text(raw.get("catalogo_nombre"), "catalogo_nombre")
    if raw[code_field] != code or raw["catalogo_nombre"] != expected_name:
        raise CommandError("Código o catalogo_nombre no coincide exactamente con el catálogo.")
    model = PointProduct if domain == "producto" else Insumo
    source_query = model.objects.select_for_update() if bloquear else model.objects
    source = source_query.filter(pk=ident).first()
    if source is None:
        raise CommandError(f"{domain} catalogo_id={ident} no existe.")
    active = source.active if domain == "producto" else source.activo
    if not active:
        raise CommandError(f"{domain} catalogo_id={ident} está inactivo.")
    actual_code = source.sku if domain == "producto" else source.codigo_point
    actual_name = source.name if domain == "producto" else source.nombre
    if actual_code != code or actual_name != expected_name:
        raise CommandError(
            f"La identidad de {domain} catalogo_id={ident} no coincide con código y nombre del catálogo."
        )
    if domain == "producto" and "external_id" in raw:
        external_id = _text(raw["external_id"], "external_id", 120)
        if raw["external_id"] != external_id or source.external_id != external_id:
            raise CommandError(
                f"El external_id de producto catalogo_id={ident} no coincide con el catálogo."
            )
    try:
        amount = _cantidad(raw.get("cantidad"))
        frozen = resolver_articulo_conteo(("p" if domain == "producto" else "i") + str(ident))
    except ConteoError as exc:
        raise CommandError("; ".join(exc.messages)) from exc
    if amount is None:
        raise CommandError("cantidad está blanca o ausente.")
    if frozen.get("codigo") != actual_code or frozen.get("nombre") != actual_name:
        raise CommandError("El catálogo cambió durante la validación del artículo.")
    current = model.objects.filter(pk=ident).first()
    current_active = current and (current.active if domain == "producto" else current.activo)
    current_code = current and (current.sku if domain == "producto" else current.codigo_point)
    current_name = current and (current.name if domain == "producto" else current.nombre)
    if (
        not current_active
        or current_code != code
        or current_name != expected_name
        or (
            domain == "producto"
            and "external_id" in raw
            and current.external_id != raw["external_id"]
        )
    ):
        raise CommandError("El catálogo cambió durante la validación del artículo.")
    return {
        "domain": domain,
        "catalogo_id": ident,
        "cantidad": amount,
        "hoja": _text(raw.get("hoja"), "hoja"),
        "celda": _text(raw.get("celda"), "celda", 40),
        "frozen": frozen,
    }


def validar_payload(payload, *, bloquear=False):
    if not isinstance(payload, dict) or set(payload) != {"fuente", "fecha", "actor", "sucursales"}:
        raise CommandError("El payload debe contener fuente, fecha, actor y sucursales.")
    source = payload["fuente"]
    if not isinstance(source, dict) or set(source) != {
        "nombre", "sha256_declarado", "verificacion_externa",
    }:
        raise CommandError(
            "fuente debe contener nombre, sha256_declarado y verificacion_externa."
        )
    verification = source["verificacion_externa"]
    if not isinstance(verification, dict) or set(verification) != {"metodo", "verificado_por"}:
        raise CommandError(
            "fuente.verificacion_externa debe contener metodo y verificado_por."
        )
    source = {
        "nombre": _text(source.get("nombre"), "fuente.nombre"),
        "sha256_declarado": _text(
            source.get("sha256_declarado"), "fuente.sha256_declarado", 64
        ),
        "verificacion_externa": {
            "metodo": _text(verification.get("metodo"), "fuente.verificacion_externa.metodo"),
            "verificado_por": _text(
                verification.get("verificado_por"),
                "fuente.verificacion_externa.verificado_por",
            ),
        },
    }
    if not re.fullmatch(r"[0-9a-f]{64}", source["sha256_declarado"]):
        raise CommandError(
            "fuente.sha256_declarado debe ser hexadecimal en minúsculas."
        )
    if not isinstance(payload["fecha"], str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}", payload["fecha"]
    ):
        raise CommandError("fecha debe usar el formato YYYY-MM-DD.")
    try:
        count_date = date.fromisoformat(payload["fecha"])
    except (TypeError, ValueError) as exc:
        raise CommandError("fecha debe usar el formato YYYY-MM-DD.") from exc
    if count_date >= timezone.localdate():
        raise CommandError("fecha debe ser anterior a la fecha local actual.")
    username = _text(payload["actor"], "actor", 150)
    actor = get_user_model().objects.filter(username=username, is_active=True).first()
    if actor is None:
        raise CommandError("El actor no existe o está inactivo.")
    if not puede_coordinar(actor):
        raise CommandError("El actor no tiene permiso vigente para coordinar conteos.")
    branches = payload["sucursales"]
    if not isinstance(branches, list) or len(branches) != 4:
        raise CommandError("El payload debe contener exactamente cuatro sucursales.")
    codes = []
    for raw_branch in branches:
        if not isinstance(raw_branch, dict) or set(raw_branch) != {"codigo", "conteos"}:
            raise CommandError("Cada sucursal debe contener exclusivamente codigo y conteos.")
        codes.append(_text(raw_branch["codigo"], "sucursal.codigo", 20))
    if len(set(codes)) != 4:
        raise CommandError("Las cuatro sucursales deben ser distintas.")
    query = Sucursal.objects.select_for_update() if bloquear else Sucursal.objects
    catalog = {branch.codigo: branch for branch in query.filter(codigo__in=sorted(codes))}
    if len(catalog) != 4:
        raise CommandError("Alguna sucursal no existe.")

    payload_hash = _fingerprint(payload)
    plans = []
    for raw_branch in branches:
        branch = catalog[raw_branch["codigo"].strip()]
        if not branch.esta_operativa(count_date):
            raise CommandError(f"La sucursal {branch.codigo} está inactiva para la fecha indicada.")
        raw_lines = raw_branch["conteos"]
        if not isinstance(raw_lines, list) or not raw_lines or len(raw_lines) > 2000:
            raise CommandError("Cada sucursal requiere entre 1 y 2000 conteos.")
        lines = [_resolver_linea(raw, bloquear=bloquear) for raw in raw_lines]
        identities = [(line["domain"], line["catalogo_id"]) for line in lines]
        physical_codes = [line["frozen"]["codigo"] for line in lines]
        if len(identities) != len(set(identities)) or len(physical_codes) != len(set(physical_codes)):
            raise CommandError(f"La sucursal {branch.codigo} contiene un artículo duplicado.")
        request_id = uuid5(
            IMPORT_NAMESPACE,
            f'{source["sha256_declarado"]}:{count_date.isoformat()}:{branch.codigo}',
        )
        previous = ConteoSucursal.objects.filter(request_id=request_id).first()
        if previous and previous.payload_hash != payload_hash:
            raise CommandError("La misma importación ya se usó con otros datos.")
        conflicts = ConteoSucursal.objects.filter(
            sucursal=branch, fecha=count_date
        ).exclude(estado="CANCELADO")
        if previous:
            conflicts = conflicts.exclude(pk=previous.pk)
        if conflicts.exists():
            raise CommandError(
                f"Ya existe un ConteoSucursal no cancelado para {branch.codigo} y {count_date}."
            )
        plans.append(
            {
                "sucursal": branch,
                "lineas": lines,
                "request_id": request_id,
                "previous": previous,
            }
        )
    return source, count_date, actor, payload_hash, plans


def aplicar_payload(payload):
    with transaction.atomic():
        source, count_date, actor, payload_hash, plans = validar_payload(payload, bloquear=True)
        created = 0
        for plan in plans:
            if plan["previous"]:
                continue
            branch = plan["sucursal"]
            count = ConteoSucursal.objects.create(
                sucursal=branch,
                responsable=actor,
                creado_por=actor,
                fecha=count_date,
                titulo=f'Conteo histórico · {source["nombre"]}'[:180],
                estado="ENVIADO",
                observaciones=(
                    f'Importación histórica desde {source["nombre"]}; '
                    f'digest declarada {source["sha256_declarado"]}; '
                    "verificación externa declarada "
                    f'{source["verificacion_externa"]["metodo"]} por '
                    f'{source["verificacion_externa"]["verificado_por"]}; '
                    "sin hora física comprobable; no ajusta Point."
                ),
                request_id=plan["request_id"],
                payload_hash=payload_hash,
                enviado_en=timezone.now(),
            )
            provenance = []
            products = 0
            for item in plan["lineas"]:
                line = LineaConteoSucursal.objects.create(conteo=count, **item["frozen"])
                LecturaConteoSucursal.objects.create(
                    linea=line,
                    ronda=1,
                    cantidad=item["cantidad"],
                    incidencia=(
                        f'Importación histórica: {source["nombre"]} '
                        f'(digest declarada {source["sha256_declarado"]}), '
                        f'hoja {item["hoja"]}, celda {item["celda"]}.'
                    ),
                    actor=actor,
                )
                products += item["domain"] == "producto"
                provenance.append(
                    {
                        "domain": item["domain"],
                        "catalogo_id": item["catalogo_id"],
                        "codigo": item["frozen"]["codigo"],
                        "hoja": item["hoja"],
                        "celda": item["celda"],
                    }
                )
            summary = {
                "lineas": len(plan["lineas"]),
                "productos": products,
                "insumos": len(plan["lineas"]) - products,
            }
            EventoConteoSucursal.objects.create(
                conteo=count,
                actor=actor,
                action="importacion_historica",
                payload={
                    "fuente": source,
                    "fecha": count_date.isoformat(),
                    "procedencia": {
                        "archivo": source["nombre"],
                        "sha256_declarado": source["sha256_declarado"],
                        "verificacion_externa": source["verificacion_externa"],
                        "celdas": provenance,
                    },
                    "resumen": summary,
                },
            )
            EventoConteoSucursal.objects.create(
                conteo=count,
                actor=actor,
                action="enviar_importacion_historica",
                payload={"resumen": summary, "estado": "ENVIADO"},
            )
            created += 1
        return created, len(plans) - created


class Command(BaseCommand):
    help = "Importa evidencia histórica de conteos físicos de sucursal."

    def add_arguments(self, parser):
        parser.add_argument("archivo")
        parser.add_argument("--aplicar", action="store_true")

    def handle(self, *args, **options):
        path = Path(options["archivo"]).expanduser()
        try:
            raw = sys.stdin.read() if options["archivo"] == "-" else path.read_text(encoding="utf-8")
            payload = json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CommandError(f"No se pudo leer el payload JSON: {exc}") from exc
        if not options["aplicar"]:
            _, _, _, _, plans = validar_payload(payload)
            existing = sum(plan["previous"] is not None for plan in plans)
            self.stdout.write(
                self.style.WARNING(
                    f"DRY-RUN válido: {len(plans) - existing} por crear, {existing} ya importados; cero escrituras."
                )
            )
            return
        created, existing = aplicar_payload(payload)
        self.stdout.write(
            self.style.SUCCESS(f"Importación completa: {created} creados, {existing} ya importados.")
        )

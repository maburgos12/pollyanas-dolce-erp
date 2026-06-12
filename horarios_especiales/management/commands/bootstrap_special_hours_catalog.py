from __future__ import annotations

from django.core.management.base import BaseCommand

from core.models import Sucursal
from horarios_especiales.models import SucursalAlias, SucursalPlataformaExterna, normalize_text


CATALOG = [
    {
        "codigo": "MATRIZ",
        "aliases": ["matriz", "benigno valenzuela"],
        "google_address": "Avenida Benigno Valenzuela 101, Centro, 81000 Guasave, Sinaloa",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "17542525418474435119",
        "google_profile_fid": "8917937730700354054",
    },
    {
        "codigo": "PAYAN",
        "aliases": ["paya", "payan", "ruiz payan"],
        "google_address": "Blvd. R. Ruiz Payán 184 L2, Del Bosque, 81040 Guasave, Sin.",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "2151298028727769596",
        "google_profile_fid": "15653279129994973088",
    },
    {
        "codigo": "LAS_GLORIAS",
        "aliases": ["glorias", "las glorias", "plaza las glorias"],
        "google_address": "Francisco González Bocanegra 87, Plaza Las Glorias, Local 4, Las Huertas, 81075 Guasave, Sin.",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "1078777384212545011",
        "google_profile_fid": "12185957855944108749",
    },
    {
        "codigo": "PLAZA_NIO",
        "aliases": ["nio", "plaza nio", "plaza nío"],
        "google_address": "Paseo Miguel Leyson Perez 133, Plaza Nio, Local 9, Segunda Planta, 81020 Guasave, Sin.",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "13794335086211172573",
        "google_profile_fid": "15747181080996186791",
    },
    {
        "codigo": "LEYVA",
        "aliases": ["leyva", "gabriel leyva", "la once"],
        "google_address": "Calle Jose Maria Pino Suarez, Esquina con Blvd Guadalupe Victoria, La Once, 81120 Gabriel Leyva Solano, Sin.",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "14387417508861554226",
        "google_profile_fid": "13515824196438978003",
    },
    {
        "codigo": "COLOSIO",
        "aliases": ["colosio"],
        "google_address": "Luis Donaldo Colosio 81, 2 De Octubre, 81017 Guasave, Sin.",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "4068572237675605650",
        "google_profile_fid": "5459320049667594862",
    },
    {
        "codigo": "EL_TUNEL",
        "aliases": ["tunel", "el tunel", "palmillas"],
        "google_address": "Juan San Millán S/N, Las Palmillas, 81048 Guasave, Sin.",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "14362456113890332669",
        "google_profile_fid": "1541831620857546731",
    },
    {
        "codigo": "CRUCERO",
        "aliases": ["crucero", "ninos heroes", "niños heroes"],
        "google_address": "Calzada Niños Heroes #1000, Centro, 81000 Guasave, Sin.",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "9407916755148967590",
        "google_profile_fid": "15874636915267306704",
    },
    {
        "codigo": "GUAMUCHIL",
        "aliases": ["guamuchil", "guamúchil", "antonio rosales"],
        "google_address": "Av Antonio Rosales 627, 81400 Guamuchil, Sin.",
        "google_group_account_id": "113534506057208552108",
        "google_location_id": "13637818934523822761",
        "google_profile_fid": "8983561737164135539",
    },
]


class Command(BaseCommand):
    help = "Crea alias operativos y configuraciones base de Google Business Profile para horarios especiales."

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios. Sin esta bandera corre en dry-run.",
        )

    def handle(self, *args, **options):
        apply_changes = bool(options.get("apply"))
        created_aliases = 0
        updated_aliases = 0
        created_configs = 0
        updated_configs = 0
        missing_branches: list[str] = []
        conflicts: list[str] = []
        inactive_branches: list[str] = []

        for item in CATALOG:
            codigo = item["codigo"]
            branch = Sucursal.objects.filter(codigo=codigo).first()
            if not branch:
                missing_branches.append(codigo)
                continue
            if not branch.activa:
                inactive_branches.append(codigo)

            for alias_value in item["aliases"]:
                normalized = normalize_text(alias_value)
                alias = SucursalAlias.objects.filter(alias_normalizado=normalized).first()
                if alias is None:
                    if apply_changes:
                        SucursalAlias.objects.create(
                            sucursal=branch,
                            alias=alias_value,
                            source=SucursalAlias.SOURCE_MANUAL,
                            is_active=True,
                        )
                    created_aliases += 1
                    continue

                if alias.sucursal_id != branch.id:
                    conflicts.append(f"{alias_value} -> {alias.sucursal.codigo} (esperado {branch.codigo})")
                    continue

                if alias.alias != alias_value or not alias.is_active or alias.source != SucursalAlias.SOURCE_MANUAL:
                    if apply_changes:
                        alias.alias = alias_value
                        alias.is_active = True
                        alias.source = SucursalAlias.SOURCE_MANUAL
                        alias.save(update_fields=["alias", "is_active", "source", "alias_normalizado", "updated_at"])
                    updated_aliases += 1

            config_defaults = {
                "settings_json": {
                    "bootstrap_source": "bootstrap_special_hours_catalog",
                    "google_business_profile": {
                        "expected_store_name": "Pollyana's Dolce",
                        "expected_address": item["google_address"],
                        "profile_fid": item["google_profile_fid"],
                        "profile_url": f"https://business.google.com/n/{item['google_location_id']}/profile?fid={item['google_profile_fid']}",
                    },
                },
                "external_account_id": item["google_group_account_id"],
                "external_location_id": item["google_location_id"],
                "external_location_name": f"locations/{item['google_location_id']}",
                "is_active": True,
            }
            config = SucursalPlataformaExterna.objects.filter(
                sucursal=branch,
                platform=SucursalPlataformaExterna.PLATFORM_GOOGLE,
            ).first()
            if config is None:
                if apply_changes:
                    SucursalPlataformaExterna.objects.create(
                        sucursal=branch,
                        platform=SucursalPlataformaExterna.PLATFORM_GOOGLE,
                        settings_json=config_defaults["settings_json"],
                        external_account_id=config_defaults["external_account_id"],
                        external_location_id=config_defaults["external_location_id"],
                        external_location_name=config_defaults["external_location_name"],
                        is_active=True,
                    )
                created_configs += 1
                continue

            merged_settings = dict(config.settings_json or {})
            merged_settings.setdefault("bootstrap_source", "bootstrap_special_hours_catalog")
            google_payload = dict(merged_settings.get("google_business_profile") or {})
            google_payload.setdefault("expected_store_name", "Pollyana's Dolce")
            google_payload["expected_address"] = item["google_address"]
            google_payload["profile_fid"] = item["google_profile_fid"]
            google_payload["profile_url"] = f"https://business.google.com/n/{item['google_location_id']}/profile?fid={item['google_profile_fid']}"
            merged_settings["google_business_profile"] = google_payload

            if (
                merged_settings != (config.settings_json or {})
                or not config.is_active
                or config.external_account_id != item["google_group_account_id"]
                or config.external_location_id != item["google_location_id"]
                or config.external_location_name != f"locations/{item['google_location_id']}"
            ):
                if apply_changes:
                    config.settings_json = merged_settings
                    config.is_active = True
                    config.external_account_id = item["google_group_account_id"]
                    config.external_location_id = item["google_location_id"]
                    config.external_location_name = f"locations/{item['google_location_id']}"
                    config.save(
                        update_fields=[
                            "settings_json",
                            "is_active",
                            "external_account_id",
                            "external_location_id",
                            "external_location_name",
                            "updated_at",
                        ]
                    )
                updated_configs += 1

        self.stdout.write("Bootstrap horarios especiales")
        self.stdout.write(f"  - aliases creados: {created_aliases}")
        self.stdout.write(f"  - aliases actualizados: {updated_aliases}")
        self.stdout.write(f"  - configs creadas: {created_configs}")
        self.stdout.write(f"  - configs actualizadas: {updated_configs}")
        if missing_branches:
            self.stdout.write("  - sucursales faltantes en ERP:")
            for code in missing_branches:
                self.stdout.write(f"    * {code}")
        if inactive_branches:
            self.stdout.write("  - sucursales inactivas en ERP (no se reactivan automáticamente):")
            for code in inactive_branches:
                self.stdout.write(f"    * {code}")
        if conflicts:
            self.stdout.write("  - conflictos de alias:")
            for row in conflicts:
                self.stdout.write(f"    * {row}")
        if not apply_changes:
            self.stdout.write("Dry-run: no se escribieron cambios. Usa --apply para confirmar.")

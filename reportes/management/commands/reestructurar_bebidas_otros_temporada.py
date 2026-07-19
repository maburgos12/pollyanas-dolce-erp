"""Reestructura el renglón de Ventas "BEBIDAS/OTROS · ESPECIAL/TEMPORADA".

Aprobado por dirección (2026-07-18): el renglón mixto se separa en tres
renglones automatizables por reglas VENTA_POS del CSV de mapeo:

- "Bebidas"              ← fusiona los renglones "Coca-cola" y "TE"
                           (categorías Point Coca-cola, TE y Café).
- "Otros"                ← renglón nuevo (categoría Point "Otros postres").
- "Especiales/Temporada" ← renombra el renglón mixto (conserva su ppto y
                           legado) y fusiona "GALLETA · ESPECIAL/TEMPORADA"
                           (su real legado era justamente Galleta M&MS).

Por cada mes los presupuestos se SUMAN y los reales AUTO/legado se suman;
una captura MANUAL nunca se toca (se reporta y esa línea no fusiona real).
Idempotente y con --dry-run. Después de correrlo: seed_reglas_fuente_rubro
y consolidar_presupuesto_real.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from reportes.models import LineaPresupuestoMensual, ReglaFuenteRubro, RubroPresupuesto
from reportes.services_presupuesto_real import AUTO_PREFIX, es_manual

TEMPORADA = "Especiales/Temporada"
BEBIDAS = "Bebidas"
OTROS = "Otros"


class Command(BaseCommand):
    help = "Separa BEBIDAS/OTROS · ESPECIAL/TEMPORADA en Bebidas / Otros / Especiales-Temporada."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        self.dry_run = options["dry_run"]
        with transaction.atomic():
            temporada = self._renombrar("BEBIDAS/OTROS · ESPECIAL/TEMPORADA", TEMPORADA)
            self._fusionar(temporada, "GALLETA · ESPECIAL/TEMPORADA")

            bebidas = self._renombrar("Coca-cola", BEBIDAS)
            self._fusionar(bebidas, "TE")

            self._crear_otros(periodos_como=temporada)
            if self.dry_run:
                transaction.set_rollback(True)
        modo = "DRY-RUN" if self.dry_run else "APLICADO"
        self.stdout.write(f"[{modo}] Reestructura Bebidas/Otros/Especiales-Temporada lista.")

    # ------------------------------------------------------------------ #

    def _rubro(self, concepto: str) -> RubroPresupuesto | None:
        return RubroPresupuesto.objects.filter(
            area__codigo="ventas", concepto=concepto, activo=True
        ).first()

    def _renombrar(self, origen: str, destino: str) -> RubroPresupuesto:
        rubro = self._rubro(destino)
        if rubro is not None:
            self.stdout.write(f"  '{destino}' ya existe (rubro {rubro.id}); sin renombrar.")
            return rubro
        rubro = self._rubro(origen)
        if rubro is None:
            raise CommandError(f"No existe el rubro de ventas '{origen}' ni '{destino}'.")
        metadata = dict(rubro.metadata or {})
        metadata.setdefault("nombre_excel", rubro.concepto)
        metadata["nombre_anterior"] = rubro.concepto
        rubro.concepto = destino
        rubro.metadata = metadata
        if not self.dry_run:
            rubro.save(update_fields=["concepto", "metadata", "actualizado_en"])
        self.stdout.write(f"  renombrado: '{origen}' → '{destino}' (rubro {rubro.id})")
        return rubro

    def _fusionar(self, destino: RubroPresupuesto, origen_concepto: str) -> None:
        origen = self._rubro(origen_concepto)
        if origen is None or origen.pk == destino.pk:
            self.stdout.write(f"  origen '{origen_concepto}' no encontrado o ya fusionado; omitido.")
            return
        lineas_destino = {l.periodo: l for l in destino.lineas_mensuales.all()}
        for linea in origen.lineas_mensuales.all():
            existente = lineas_destino.get(linea.periodo)
            if existente is None:
                if not self.dry_run:
                    LineaPresupuestoMensual.objects.create(
                        rubro=destino,
                        periodo=linea.periodo,
                        version=linea.version,
                        monto_presupuesto=linea.monto_presupuesto,
                        monto_real=linea.monto_real,
                        fuente_real=linea.fuente_real,
                        metadata={"fusionado_de": origen.id},
                    )
                continue
            existente.monto_presupuesto += linea.monto_presupuesto
            campos = ["monto_presupuesto", "actualizado_en"]
            if linea.monto_real is not None:
                if es_manual(existente.fuente_real) or es_manual(linea.fuente_real):
                    self.stdout.write(self.style.WARNING(
                        f"  {linea.periodo:%Y-%m}: real MANUAL en la fusión "
                        f"'{origen_concepto}' → '{destino.concepto}'; real NO fusionado."
                    ))
                else:
                    existente.monto_real = (existente.monto_real or 0) + linea.monto_real
                    existente.fuente_real = existente.fuente_real or f"{AUTO_PREFIX}LEGADO"
                    campos += ["monto_real", "fuente_real"]
            if not self.dry_run:
                existente.save(update_fields=campos)
        origen.activo = False
        metadata = dict(origen.metadata or {})
        metadata["motivo_desactivacion"] = (
            f"fusionado en rubro {destino.id} '{destino.concepto}' (reestructura bebidas/otros/temporada)"
        )
        origen.metadata = metadata
        if not self.dry_run:
            origen.save(update_fields=["activo", "metadata", "actualizado_en"])
            # Sus reglas SEED ya no aplican; el seed también las reconciliaría.
            ReglaFuenteRubro.objects.filter(
                rubro=origen, origen=ReglaFuenteRubro.ORIGEN_SEED
            ).delete()
        self.stdout.write(f"  fusionado: '{origen_concepto}' (rubro {origen.id}) → '{destino.concepto}'")

    def _crear_otros(self, *, periodos_como: RubroPresupuesto) -> None:
        if self._rubro(OTROS) is not None:
            self.stdout.write(f"  '{OTROS}' ya existe; sin crear.")
            return
        rubro = RubroPresupuesto(
            area=periodos_como.area,
            concepto=OTROS,
            tipo=RubroPresupuesto.TIPO_INGRESO,
            sucursal=periodos_como.sucursal,
            metadata={"source": "REESTRUCTURA_BEBIDAS_OTROS_TEMPORADA"},
            creado_en=timezone.now(),
        )
        if not self.dry_run:
            rubro.save()
            for periodo in periodos_como.lineas_mensuales.values_list("periodo", flat=True):
                LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=periodo)
        self.stdout.write(f"  creado: '{OTROS}' (ppto 0; el real vivo entra por su regla POS)")

"""Correcciones y gobierno de fuentes del presupuesto vs. real."""

from dataclasses import dataclass

from django.db import transaction
from django.utils import timezone

from .models import CategoriaGasto, LineaPresupuestoMensual, ReglaFuenteRubro, RubroPresupuesto


@dataclass(frozen=True)
class CorreccionAguaPurificadaResultado:
    rubro_personal_id: int
    rubro_ingrediente_id: int
    lineas_corregidas: int
    reglas_desactivadas: int


@transaction.atomic
def corregir_agua_purificada() -> CorreccionAguaPurificadaResultado:
    """Separa el agua para personal del ingrediente y revierte solo el doble AUTO.

    No toca importes manuales ni legados. Cada valor retirado queda íntegro en
    metadata y solo se limpia cuando existe la contraparte canónica idéntica.
    """
    rubros = RubroPresupuesto.objects.select_for_update().filter(
        area__codigo="produccion",
        concepto__in=["Agua purificada", "Agua purificada para personal", "Agua Purificada"],
        activo=True,
    ).order_by("id")
    ingrediente = rubros.filter(concepto="Agua Purificada").first()
    personal = rubros.filter(
        concepto__in=["Agua purificada", "Agua purificada para personal"]
    ).order_by("id").first()
    if ingrediente is None or personal is None or ingrediente.pk == personal.pk:
        raise RubroPresupuesto.DoesNotExist(
            "Se requieren los rubros activos de agua para personal e ingrediente."
        )

    metadata = dict(personal.metadata or {})
    metadata.update(
        {
            "no_auto_consumo_mp": True,
            "fuente_unica": {
                "concepto_anterior": metadata.get("fuente_unica", {}).get(
                    "concepto_anterior", personal.concepto
                ),
                "rubro_ingrediente_id": ingrediente.pk,
                "motivo": "Separación entre consumo del personal e ingrediente de producción.",
            },
        }
    )
    personal.concepto = "Agua purificada para personal"
    personal.metadata = metadata
    personal.save(update_fields=["concepto", "metadata", "actualizado_en"])

    reglas_desactivadas = personal.reglas_fuente.filter(
        tipo_fuente=ReglaFuenteRubro.FUENTE_CONSUMO_MP,
        activa=True,
    ).update(
        activa=False,
        clave_fuente="",
        notas="Desactivada: duplicaba el ingrediente Agua Purificada.",
        actualizado_en=timezone.now(),
    )

    categoria, _ = CategoriaGasto.objects.get_or_create(
        codigo="AGUA_PURIFICADA_PERSONAL",
        defaults={
            "nombre": "Agua purificada para personal",
            "capa_objetivo": CategoriaGasto.CAPA_EMPRESA,
            "bucket": CategoriaGasto.BUCKET_INDIRECTO,
            "impacta_costo_producto": False,
            "impacta_contribucion_sucursal": False,
            "impacta_utilidad_empresa": True,
        },
    )
    for centro_tipo in ("PRODUCCION", "CEDIS"):
        regla, creada = ReglaFuenteRubro.objects.get_or_create(
            rubro=personal,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            categoria_gasto=categoria,
            filtros={"centro_tipo": centro_tipo},
            defaults={
                "origen": ReglaFuenteRubro.ORIGEN_ADMIN,
                "notas": "Recibos registrados por Administración/Compras.",
            },
        )
        if not creada and not regla.activa:
            regla.activa = True
            regla.save(update_fields=["activa", "clave_fuente", "actualizado_en"])

    lineas_corregidas = 0
    candidatas = LineaPresupuestoMensual.objects.select_for_update().filter(
        rubro=personal,
        fuente_real="AUTO:CONSUMO_MP",
    )
    for linea in candidatas:
        existe_canonica = LineaPresupuestoMensual.objects.filter(
            rubro=ingrediente,
            periodo=linea.periodo,
            version=linea.version,
            fuente_real="AUTO:CONSUMO_MP",
            monto_real=linea.monto_real,
        ).exists()
        if not existe_canonica:
            continue
        linea_metadata = dict(linea.metadata or {})
        if "correccion_fuente_unica" in linea_metadata:
            continue
        linea_metadata["correccion_fuente_unica"] = {
            "monto_anterior": str(linea.monto_real),
            "fuente_anterior": linea.fuente_real,
            "rubro_canonico_id": ingrediente.pk,
            "motivo": "Doble lectura del mismo consumo de agua ingrediente.",
        }
        linea.monto_real = None
        linea.fuente_real = ""
        linea.metadata = linea_metadata
        linea.save(update_fields=["monto_real", "fuente_real", "metadata", "actualizado_en"])
        lineas_corregidas += 1

    return CorreccionAguaPurificadaResultado(
        rubro_personal_id=personal.pk,
        rubro_ingrediente_id=ingrediente.pk,
        lineas_corregidas=lineas_corregidas,
        reglas_desactivadas=reglas_desactivadas,
    )

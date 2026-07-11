from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import NamedTuple

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.utils import timezone

from core.access import can_manage_module, can_view_module
from inventario.models import (
    ALMACEN_LABELS,
    ExistenciaInsumo,
    LoteProduccion,
    MovimientoInventario,
    normalizar_codigo_lote,
    UBICACION_ARMADO,
    UBICACION_CFP_1_1,
)
from inventario.services_existencias import aplicar_delta
from maestros.models import Insumo, UnidadMedida
from recetas.models import Receta
from recetas.utils.costeo_snapshot import preparation_recipe_matches_insumo
from operacion.models import BitacoraOperativa


class CierreHornosResult(NamedTuple):
    lotes_creados: int


class EntregarArmadoResult(NamedTuple):
    asignaciones: list  # [(lote_id, cantidad), ...]


def _cantidad_positiva(cantidad) -> Decimal:
    try:
        value = Decimal(str(cantidad))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("La cantidad debe ser numérica.") from exc
    if not value.is_finite() or value <= 0:
        raise ValidationError("La cantidad debe ser finita y mayor que cero.")
    return value


def _fecha_operativa(fecha_elaboracion: date | datetime | None) -> tuple[datetime, str | None]:
    if fecha_elaboracion is None:
        return timezone.now(), None
    if isinstance(fecha_elaboracion, datetime):
        value = timezone.make_aware(fecha_elaboracion) if timezone.is_naive(fecha_elaboracion) else fecha_elaboracion
        normalized = value.astimezone(timezone.get_current_timezone())
        if normalized.date() > timezone.localdate():
            raise ValidationError("La fecha de elaboración no puede ser futura.")
        return normalized, normalized.isoformat()
    if not isinstance(fecha_elaboracion, date):
        raise ValidationError("La fecha de elaboración no es válida.")
    if fecha_elaboracion > timezone.localdate():
        raise ValidationError("La fecha de elaboración no puede ser futura.")
    value = timezone.make_aware(datetime.combine(fecha_elaboracion, time(hour=12)))
    return value, value.isoformat()


def _source_hash_apertura(insumo: Insumo, ubicacion: str) -> str:
    raw = f"bitacoras:apertura:{insumo.pk}:{ubicacion}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _validar_apertura_existente(
    movimiento: MovimientoInventario,
    *,
    receta: Receta,
    insumo: Insumo,
    cantidad: Decimal,
    unidad: UnidadMedida,
    observaciones: str,
    fecha_normalizada: str | None,
) -> LoteProduccion:
    lote = movimiento.lote
    if not lote or any(
        (
            not lote.es_apertura,
            lote.insumo_id != insumo.pk,
            movimiento.insumo_id != insumo.pk,
            movimiento.tipo != MovimientoInventario.TIPO_ENTRADA,
            movimiento.almacen != movimiento.trazabilidad.get("ubicacion"),
            lote.receta_id != receta.pk,
            lote.cantidad_inicial != cantidad,
            lote.unidad_id != unidad.pk,
            lote.observaciones != observaciones,
            movimiento.trazabilidad.get("fecha_elaboracion") != fecha_normalizada,
        )
    ):
        raise ValidationError(
            "La apertura de este producto y ubicación ya fue aplicada y no puede editarse."
        )
    return lote


def registrar_apertura_inicial(
    *,
    receta: Receta,
    insumo: Insumo,
    cantidad,
    unidad: UnidadMedida,
    ubicacion: str,
    fecha_elaboracion: date | datetime | None,
    actor,
    observaciones: str,
) -> LoteProduccion:
    if not can_manage_module(actor, "produccion"):
        raise PermissionDenied("Se requiere permiso de gestión de Producción.")
    cantidad_decimal = _cantidad_positiva(cantidad)
    observaciones_limpias = (observaciones or "").strip()
    if not observaciones_limpias:
        raise ValidationError("La observación de apertura es obligatoria.")
    if ubicacion not in ALMACEN_LABELS:
        raise ValidationError("Selecciona una ubicación válida.")
    if not normalizar_codigo_lote(insumo.codigo_point) or not normalizar_codigo_lote(receta.codigo_point):
        raise ValidationError("El producto requiere identidad canónica de Point.")
    if receta.tipo != Receta.TIPO_PREPARACION or not preparation_recipe_matches_insumo(receta, insumo):
        raise ValidationError("El producto y su receta canónica no corresponden.")
    if not insumo.unidad_base_id or unidad.pk != insumo.unidad_base_id:
        raise ValidationError("La unidad debe coincidir con la unidad base del producto.")

    producido_en, fecha_normalizada = _fecha_operativa(fecha_elaboracion)
    source_hash = _source_hash_apertura(insumo, ubicacion)

    movimiento_existente = MovimientoInventario.objects.select_related("lote").filter(source_hash=source_hash).first()
    if movimiento_existente:
        return _validar_apertura_existente(
            movimiento_existente,
            receta=receta,
            insumo=insumo,
            cantidad=cantidad_decimal,
            unidad=unidad,
            observaciones=observaciones_limpias,
            fecha_normalizada=fecha_normalizada,
        )

    try:
        with transaction.atomic():
            movimiento_existente = (
                MovimientoInventario.objects.select_for_update()
                .filter(source_hash=source_hash)
                .first()
            )
            if movimiento_existente:
                return _validar_apertura_existente(
                    movimiento_existente,
                    receta=receta,
                    insumo=insumo,
                    cantidad=cantidad_decimal,
                    unidad=unidad,
                    observaciones=observaciones_limpias,
                    fecha_normalizada=fecha_normalizada,
                )
            existencia, _ = ExistenciaInsumo.objects.select_for_update().get_or_create(
                insumo=insumo,
                almacen=ubicacion,
            )
            if Decimal(str(existencia.stock_actual or 0)) != Decimal("0"):
                raise ValidationError(
                    "La ubicación ya tiene saldo; primero debes conciliarlo o registrar un ajuste autorizado."
                )
            lote = LoteProduccion.objects.create(
                insumo=insumo,
                receta=receta,
                cantidad_inicial=cantidad_decimal,
                unidad=unidad,
                producido_en=producido_en,
                linea_origen=None,
                creado_por=actor,
                es_apertura=True,
                observaciones=observaciones_limpias,
            )
            MovimientoInventario.objects.create(
                fecha=timezone.now(),
                tipo=MovimientoInventario.TIPO_ENTRADA,
                insumo=insumo,
                cantidad=cantidad_decimal,
                referencia=lote.codigo,
                almacen=ubicacion,
                notas=observaciones_limpias,
                registrado_por=actor.get_username(),
                source_hash=source_hash,
                lote=lote,
                linea_bitacora=None,
                registrado_por_usuario=actor,
                trazabilidad={
                    "evento": "apertura_inicial",
                    "lote": lote.codigo,
                    "ubicacion": ubicacion,
                    "fecha_elaboracion": fecha_normalizada,
                    "sin_fuente_historica": True,
                },
            )
            aplicar_delta(insumo, ubicacion, cantidad_decimal)
            return lote
    except IntegrityError as original_error:
        movimiento = MovimientoInventario.objects.select_related("lote").filter(source_hash=source_hash).first()
        if movimiento is None:
            raise
        try:
            return _validar_apertura_existente(
                movimiento,
                receta=receta,
                insumo=insumo,
                cantidad=cantidad_decimal,
                unidad=unidad,
                observaciones=observaciones_limpias,
                fecha_normalizada=fecha_normalizada,
            )
        except ValidationError:
            raise original_error


def _resolve_insumo_from_receta(receta: Receta) -> Insumo | None:
    """Resolver el insumo derivado desde una receta PREPARACION por codigo_point."""
    if not receta or receta.tipo != Receta.TIPO_PREPARACION:
        return None
    punto = (receta.codigo_point or "").strip().upper()
    if not punto:
        return None
    return Insumo.objects.filter(
        tipo_item=Insumo.TIPO_INTERNO,
        codigo_point__iexact=punto,
    ).first()


def cerrar_hornos(bitacora: BitacoraOperativa, actor) -> CierreHornosResult:
    if not can_manage_module(actor, "produccion"):
        raise PermissionDenied("Se requiere permiso de gestión de Producción.")
    if bitacora.tipo != BitacoraOperativa.TIPO_HORNOS:
        raise ValidationError("La bitacora no corresponde a Hornos.")

    with transaction.atomic():
        bitacora_locked = BitacoraOperativa.objects.select_for_update().get(pk=bitacora.pk)
        lineas = bitacora_locked.lineas.select_related("receta").all()

        lotes_creados = 0
        for linea in lineas:
            if linea.receta is None or not normalizar_codigo_lote(linea.receta.codigo_point):
                raise ValidationError("La linea requiere una receta con codigo Point.")
            if linea.receta.rendimiento_unidad_id is None:
                raise ValidationError("La preparacion requiere unidad de rendimiento.")

            insumo = _resolve_insumo_from_receta(linea.receta)
            if not insumo:
                raise ValidationError(f"No se encontró el insumo derivado de {linea.receta.nombre}.")

            cantidad = _cantidad_positiva(linea.datos.get("existencia"))
            source_hash = sha256(
                f"BITACORA:HORNOS:{linea.id}:ENTRADA:{UBICACION_CFP_1_1}".encode()
            ).hexdigest()

            movimiento, movimiento_nuevo = MovimientoInventario.objects.select_for_update().get_or_create(
                linea_bitacora=linea,
                almacen=UBICACION_CFP_1_1,
                defaults={
                    "fecha": timezone.now(),
                    "tipo": MovimientoInventario.TIPO_ENTRADA,
                    "insumo": insumo,
                    "cantidad": cantidad,
                    "referencia": "",
                    "notas": linea.observaciones,
                    "registrado_por": actor.get_username(),
                    "source_hash": source_hash,
                    "registrado_por_usuario": actor,
                    "trazabilidad": {
                        "evento": "cierre_hornos",
                        "bitacora_id": bitacora_locked.pk,
                        "linea_id": linea.id,
                    },
                },
            )

            if not movimiento_nuevo:
                if movimiento.cantidad != cantidad or movimiento.insumo_id != insumo.pk:
                    raise ValidationError(
                        "El movimiento/lote de esta línea ya existe con datos incompatibles."
                    )
            else:
                lote, lote_nuevo = LoteProduccion.objects.select_for_update().get_or_create(
                    linea_origen=linea,
                    defaults={
                        "insumo": insumo,
                        "receta": linea.receta,
                        "cantidad_inicial": cantidad,
                        "unidad": linea.receta.rendimiento_unidad,
                        "producido_en": timezone.make_aware(
                            datetime.combine(bitacora_locked.fecha, time(12, 0))
                        ),
                        "creado_por": actor,
                        "estado": LoteProduccion.DISPONIBLE,
                    },
                )
                movimiento.lote = lote
                movimiento.referencia = lote.codigo
                movimiento.save(update_fields=["lote", "referencia"])

                if lote_nuevo:
                    aplicar_delta(insumo, UBICACION_CFP_1_1, cantidad)
                    lotes_creados += 1

        bitacora_locked.estatus = BitacoraOperativa.ESTATUS_CERRADA
        bitacora_locked.cerrado_en = timezone.now()
        bitacora_locked.actualizado_en = timezone.now()
        bitacora_locked.save(update_fields=["estatus", "cerrado_en", "actualizado_en"])

        return CierreHornosResult(lotes_creados=lotes_creados)


def guardar_corte_ciego(bitacora: BitacoraOperativa, actor):
    """
    Sella el corte ciego de CFP 1.1 calculando esperado/diferencia por línea.
    Crea una notificación si hay diferencia != 0.
    Idempotente: llamadas posteriores no modifican ni recrean notificación.
    """
    from core.notificaciones import crear_notificaciones
    from recetas.utils.costeo_snapshot import resolve_preparation_recipe_for_insumo

    if bitacora.tipo != BitacoraOperativa.TIPO_CFP11:
        raise ValidationError("La bitácora no es de tipo CFP 1.1.")

    with transaction.atomic():
        bitacora_locked = (
            BitacoraOperativa.objects
            .select_for_update()
            .prefetch_related("lineas")
            .get(pk=bitacora.pk)
        )

        # Si ya fue sellado, no hacer nada (idempotencia)
        if bitacora_locked.conteo_guardado_en is not None:
            return

        # Validar, calcular y guardar esperado/diferencia en cada línea
        timestamp_cierre = timezone.now()
        hay_diferencia = False

        for linea in bitacora_locked.lineas.all():
            # Validar cantidad física
            cantidad_fisica_str = linea.datos.get("existencia_fisica", "").strip()
            if not cantidad_fisica_str:
                raise ValidationError(
                    f"Línea {linea.id}: Existencia física es obligatoria."
                )
            try:
                cantidad_fisica = Decimal(cantidad_fisica_str)
            except (InvalidOperation, TypeError, ValueError):
                raise ValidationError(
                    f"Línea {linea.id}: Existencia física debe ser numérica."
                )
            if not cantidad_fisica.is_finite() or cantidad_fisica < 0:
                raise ValidationError(
                    f"Línea {linea.id}: Existencia física debe ser finita y no negativa."
                )

            # Buscar insumo desde receta si hay producto
            if linea.receta:
                insumo = _resolve_insumo_from_receta(linea.receta)
            else:
                insumo = None

            # Calcular esperado (suma ENTRADA-AJUSTE hasta cierre)
            esperado = Decimal("0")
            if insumo:
                movimientos = MovimientoInventario.objects.filter(
                    insumo=insumo,
                    almacen=UBICACION_CFP_1_1,
                ).exclude(fecha__gt=timestamp_cierre)
                for mov in movimientos:
                    if mov.tipo == MovimientoInventario.TIPO_ENTRADA:
                        esperado += mov.cantidad
                    elif mov.tipo == MovimientoInventario.TIPO_SALIDA:
                        esperado -= mov.cantidad
                    elif mov.tipo == MovimientoInventario.TIPO_AJUSTE:
                        if mov.trazabilidad.get("es_positivo"):
                            esperado += mov.cantidad
                        else:
                            esperado -= mov.cantidad

            # Calcular diferencia y guardar en linea.datos (inmutable)
            diferencia = cantidad_fisica - esperado
            linea_datos_nuevo = dict(linea.datos)
            # Normalizar para eliminar ceros finales
            linea_datos_nuevo["esperado"] = str(esperado.normalize())
            linea_datos_nuevo["diferencia"] = str(diferencia.normalize())
            linea.datos = linea_datos_nuevo
            linea.save(update_fields=["datos"])

            if diferencia != Decimal("0"):
                hay_diferencia = True

        # Crear UNA SOLA notificación si hay diferencia
        if hay_diferencia:
            from core.models import UserModuleAccess, Notificacion
            from core.access import ACCESS_MANAGE

            jefes_produccion = (
                UserModuleAccess.objects
                .filter(module="produccion", access__gte=ACCESS_MANAGE)
                .values_list("user", flat=True)
            )
            if jefes_produccion:
                crear_notificaciones(
                    jefes_produccion,
                    titulo="Diferencia en corte ciego CFP 1.1",
                    mensaje=f"Corte ciego del {bitacora_locked.fecha} tiene diferencias. Revisar.",
                    tipo=Notificacion.TIPO_SISTEMA,
                    objeto_tipo="operacion.BitacoraOperativa",
                    objeto_id=bitacora_locked.id,
                )

        # Sellar con timestamp y usuario
        bitacora_locked.conteo_guardado_en = timestamp_cierre
        bitacora_locked.conteo_guardado_por = actor
        bitacora_locked.save(update_fields=["conteo_guardado_en", "conteo_guardado_por", "actualizado_en"])


def _calculate_lot_availability(lote: LoteProduccion, ubicacion: str) -> Decimal:
    """Calculate available quantity for a lot at a location from MovimientoInventario."""
    disponible = Decimal("0")
    movimientos = MovimientoInventario.objects.filter(lote=lote, almacen=ubicacion)
    for mov in movimientos:
        if mov.tipo == MovimientoInventario.TIPO_ENTRADA:
            disponible += mov.cantidad
        elif mov.tipo == MovimientoInventario.TIPO_SALIDA:
            disponible -= mov.cantidad
    return disponible


def entregar_a_armado(
    insumo: Insumo,
    cantidad,
    linea: "BitacoraOperativaLinea",
    actor,
    lote_excepcion: LoteProduccion | None = None,
    motivo_excepcion_fifo: str | None = None,
) -> EntregarArmadoResult:
    """
    Atomically transfer lots from CFP_1_1 to ARMADO using FIFO.

    - Selects DISPONIBLE lots ordered by producido_en, id
    - Creates SALIDA (CFP_1_1) and ENTRADA (ARMADO) movements
    - Rolls back without creating any movements if total insufficient
    - Supports FIFO exception: requires motivo_excepcion_fifo and manage permission
    - Idempotent via transfer_id_hash: calling twice returns same result
    """
    if not can_view_module(actor, "produccion"):
        raise PermissionDenied("Se requiere acceso a Producción.")

    cantidad_decimal = _cantidad_positiva(cantidad)

    # Hallazgo #1: Validar parámetros ANTES de cualquier lock/transacción
    if lote_excepcion and not motivo_excepcion_fifo:
        raise ValidationError("La excepción FIFO requiere un motivo.")

    if lote_excepcion and not can_manage_module(actor, "produccion"):
        raise PermissionDenied("La excepción FIFO requiere permiso de gestión de Producción.")

    # Hallazgo #2: transfer_id_hash estable: basado en parámetros que identifican UNA transferencia lógica
    # Incluye linea.id para que cada línea de bitacora tenga su propia transferencia
    # Idempotencia: si llamas EXACTAMENTE con mismos parámetros 2 veces, devuelve igual
    transfer_hash_base = f"ENTREGAR_ARMADO:LINEA:{linea.id}:INSUMO:{insumo.id}:CFP_1_1->ARMADO"
    transfer_id_hash = sha256(transfer_hash_base.encode()).hexdigest()[:16]

    with transaction.atomic():
        from operacion.models import BitacoraOperativaLinea

        linea = BitacoraOperativaLinea.objects.select_for_update().get(pk=linea.pk)
        movimientos_transferencia = [
            mov
            for mov in MovimientoInventario.objects.select_for_update().filter(insumo=insumo)
            if (mov.trazabilidad or {}).get("transfer_id") == transfer_id_hash
        ]
        if movimientos_transferencia:
            salidas = [m for m in movimientos_transferencia if m.tipo == MovimientoInventario.TIPO_SALIDA and m.almacen == UBICACION_CFP_1_1]
            entradas = [m for m in movimientos_transferencia if m.tipo == MovimientoInventario.TIPO_ENTRADA and m.almacen == UBICACION_ARMADO]
            pares_entrada = {
                (m.lote_id, m.cantidad, (m.trazabilidad or {}).get("linea_transferencia_id"))
                for m in entradas
            }
            if (
                not salidas
                or len(salidas) != len(entradas)
                or sum((m.cantidad for m in salidas), Decimal("0")) != cantidad_decimal
                or any((m.lote_id, m.cantidad, linea.id) not in pares_entrada for m in salidas)
                or any((m.trazabilidad or {}).get("linea_transferencia_id") != linea.id for m in salidas)
            ):
                raise ValidationError("La transferencia existente está incompleta o no coincide con la solicitud.")
            salidas.sort(key=lambda m: (m.lote.producido_en, m.lote_id))
            return EntregarArmadoResult(asignaciones=[(m.lote_id, m.cantidad) for m in salidas])

        lotes_query = LoteProduccion.objects.select_for_update().filter(
            insumo=insumo,
            estado=LoteProduccion.DISPONIBLE,
        ).order_by("producido_en", "id")
        if lote_excepcion:
            lote_excepcion = LoteProduccion.objects.select_for_update().get(pk=lote_excepcion.pk)
            if lote_excepcion.insumo_id != insumo.pk or lote_excepcion.estado != LoteProduccion.DISPONIBLE:
                raise ValidationError("El lote de excepción no es válido para este insumo.")
            lotes_disponibles = [lote_excepcion] + list(lotes_query.exclude(pk=lote_excepcion.pk))
        else:
            lotes_disponibles = list(lotes_query)

        total_disponible = Decimal("0")
        lote_disponibilidades = {}
        for lote in lotes_disponibles:
            disp = _calculate_lot_availability(lote, UBICACION_CFP_1_1)
            if disp > 0:
                total_disponible += disp
                lote_disponibilidades[lote.id] = disp

        if total_disponible < cantidad_decimal:
            raise ValidationError(
                f"Stock insuficiente: se requieren {cantidad_decimal} pero hay {total_disponible} disponibles."
            )

        # Assign lots FIFO
        asignaciones = []
        faltante = cantidad_decimal
        for lote in lotes_disponibles:
            if faltante <= 0:
                break
            disp = lote_disponibilidades.get(lote.id, Decimal("0"))
            if disp <= 0:
                continue

            cantidad_asignar = min(faltante, disp)
            asignaciones.append((lote.id, cantidad_asignar))
            faltante -= cantidad_asignar

            salida_hash = sha256(
                f"ENTREGAR_ARMADO:SALIDA:{transfer_id_hash}:{lote.id}:{UBICACION_CFP_1_1}".encode()
            ).hexdigest()
            salida_notas = motivo_excepcion_fifo or ""
            MovimientoInventario.objects.create(
                fecha=timezone.now(),
                tipo=MovimientoInventario.TIPO_SALIDA,
                insumo=insumo,
                cantidad=cantidad_asignar,
                almacen=UBICACION_CFP_1_1,
                referencia=lote.codigo,
                notas=salida_notas,
                registrado_por=actor.get_username(),
                registrado_por_usuario=actor,
                source_hash=salida_hash,
                lote=lote,
                linea_bitacora=lote.linea_origen,
                trazabilidad={
                    "evento": "entregar_armado_salida",
                    "lote": lote.codigo,
                    "transfer_id": transfer_id_hash,
                    "linea_transferencia_id": linea.id,
                    "es_excepcion_fifo": bool(lote_excepcion and lote.id == lote_excepcion.id),
                },
            )

            # Create ENTRADA to ARMADO
            entrada_hash = sha256(
                f"ENTREGAR_ARMADO:ENTRADA:{transfer_id_hash}:{lote.id}:{UBICACION_ARMADO}".encode()
            ).hexdigest()
            entrada_notas = motivo_excepcion_fifo or ""
            MovimientoInventario.objects.create(
                fecha=timezone.now(),
                tipo=MovimientoInventario.TIPO_ENTRADA,
                insumo=insumo,
                cantidad=cantidad_asignar,
                almacen=UBICACION_ARMADO,
                referencia=lote.codigo,
                notas=entrada_notas,
                registrado_por=actor.get_username(),
                registrado_por_usuario=actor,
                source_hash=entrada_hash,
                lote=lote,
                linea_bitacora=lote.linea_origen,
                trazabilidad={
                    "evento": "entregar_armado_entrada",
                    "lote": lote.codigo,
                    "transfer_id": transfer_id_hash,
                    "linea_transferencia_id": linea.id,
                    "es_excepcion_fifo": bool(lote_excepcion and lote.id == lote_excepcion.id),
                },
            )

        # Update stocks per location (once per location)
        aplicar_delta(insumo, UBICACION_CFP_1_1, -cantidad_decimal)
        aplicar_delta(insumo, UBICACION_ARMADO, cantidad_decimal)

        return EntregarArmadoResult(asignaciones=asignaciones)

"""Correcciones auditables y registro de compra, independientes de la recepción."""
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import UploadedFile
from django.db import transaction
from django.utils import timezone

from .forms_cotizaciones import CotizacionDepartamentalForm
from .models import (
    CompraRealizadaDepartamental, CompromisoCompraDepartamental, CotizacionCompraDepartamental,
    EventoCompraDepartamental, HistorialCompraDepartamental, HistorialCotizacionDepartamental,
    IntentoCompraDepartamental, ItemCompraDepartamental, LineaOrdenCompraDepartamental, RecepcionItemDepartamental,
)
from .services_avisos_compra import programar_avisos

CAMPOS_MONETARIOS = ('cantidad_ofertada', 'costo_unitario', 'descuento', 'impuestos', 'envio', 'instalacion', 'otros_cargos')
ESTADOS_CERRADOS = ('COMPRADO', 'RECIBIDO_PARCIAL', 'PENDIENTE_CONFIRMACION', 'RECIBIDO_CONFORME', 'RECHAZADO', 'CANCELADO')


def tiene_compra_o_recepcion(item):
    return (CompraRealizadaDepartamental.objects.filter(intento__item=item, intento__estado="VIGENTE").exists()
            or RecepcionItemDepartamental.objects.filter(
                linea_orden__intento__item=item, linea_orden__intento__estado="VIGENTE"
            ).exists())


def tiene_recepcion_historica(item):
    return RecepcionItemDepartamental.objects.filter(linea_orden__item=item).exists()


def validar_edicion(item):
    if (item.estado in ESTADOS_CERRADOS or item.solicitud.estado in ('BORRADOR', 'CANCELADA', 'COMPLETADA')
            or tiene_compra_o_recepcion(item) or tiene_recepcion_historica(item)):
        raise ValidationError('No puedes editar esta cotización: el artículo está cerrado, comprado o tiene entregas registradas.')


def snapshot_cotizacion(cotizacion):
    values = {}
    for name in CotizacionDepartamentalForm.Meta.fields:
        value = getattr(cotizacion, name)
        values[name] = (format(value, f'.{cotizacion._meta.get_field(name).decimal_places}f')
                        if isinstance(value, Decimal) else str(value) if value is not None else '')
    values['proveedor_id'] = cotizacion.proveedor_id
    values['total_adquisicion'] = str(cotizacion.total_adquisicion)
    values['version'] = cotizacion.version
    return values


def sincronizar_linea_orden(item, cotizacion, *, actor):
    linea = LineaOrdenCompraDepartamental.objects.filter(
        intento__item=item, intento__estado=IntentoCompraDepartamental.ESTADO_VIGENTE
    ).select_related("orden").first()
    if linea:
        if linea.cotizacion_id != cotizacion.pk or linea.orden.proveedor_id != cotizacion.proveedor_id:
            raise ValidationError('La orden está vinculada a otra cotización o proveedor. Revisa la orden antes de continuar.')
        anterior = str(linea.total)
        linea.costo_unitario = cotizacion.costo_unitario
        linea.total = cotizacion.total_adquisicion
        linea.save(update_fields=['costo_unitario', 'total'])
        EventoCompraDepartamental.objects.create(
            solicitud=item.solicitud, item=item, actor=actor, tipo='ORDEN_ACTUALIZADA',
            detalle=f'{linea.orden.folio}: importe anterior ${anterior}; importe autorizado ${linea.total}.',
        )
    return linea


@transaction.atomic
def editar_cotizacion(cotizacion, *, datos, version, motivo, actor):
    from .services_departamentales import (
        evaluar_presupuesto_item, liberar_compromiso_del_flujo, reservar_compromiso_del_flujo,
    )

    item = ItemCompraDepartamental.objects.select_for_update().select_related('solicitud__area').get(pk=cotizacion.item_id)
    cotizacion = CotizacionCompraDepartamental.objects.select_for_update().get(pk=cotizacion.pk)
    validar_edicion(item)
    if cotizacion.version != version:
        raise ValidationError('Otra persona actualizó esta cotización. Recarga la página y revisa los cambios antes de guardar.')
    if not motivo.strip():
        raise ValidationError('Escribe el motivo del cambio.')
    antes = snapshot_cotizacion(cotizacion)
    total_anterior = cotizacion.total_adquisicion
    linea = LineaOrdenCompraDepartamental.objects.filter(
        intento__item=item, intento__estado=IntentoCompraDepartamental.ESTADO_VIGENTE
    ).first()
    if linea and (datos.get('proveedor', cotizacion.proveedor).pk != cotizacion.proveedor_id
                  or datos.get('cantidad_ofertada', cotizacion.cantidad_ofertada) != cotizacion.cantidad_ofertada):
        raise ValidationError('La orden ya existe. Conserva el proveedor y la cantidad ofertada; puedes corregir los importes y observaciones.')
    for name in CotizacionDepartamentalForm.Meta.fields:
        if name in datos:
            setattr(cotizacion, name, datos[name])
    cotizacion.version += 1
    cotizacion.full_clean()
    cotizacion.save()
    despues = snapshot_cotizacion(cotizacion)
    monetario = any(antes[name] != despues[name] for name in CAMPOS_MONETARIOS)
    if cotizacion.seleccionada and monetario:
        evaluacion = evaluar_presupuesto_item(item, cotizacion.total_adquisicion)
        # Una reducción conserva la autorización existente cuando el presupuesto
        # no es calculable; un incremento siempre exige una nueva decisión.
        exceso_conocido = evaluacion.calculable and evaluacion.exceso > 0
        requiere_dg = (cotizacion.total_adquisicion > total_anterior or exceso_conocido
                       or item.estado not in ('AUTORIZADO', 'ORDENADO'))
        if requiere_dg:
            item.estado = ItemCompraDepartamental.ESTADO_ESPERANDO_DG
            item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_DG
            liberar_compromiso_del_flujo(item, monto=cotizacion.total_adquisicion)
        else:
            item.estado = ItemCompraDepartamental.ESTADO_ORDENADO if linea else ItemCompraDepartamental.ESTADO_AUTORIZADO
            item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
            reservar_compromiso_del_flujo(
                item, cotizacion, cotizacion.total_adquisicion,
                formalizado_en=timezone.now() if linea else None,
            )
            sincronizar_linea_orden(item, cotizacion, actor=actor)
        item.save(update_fields=['estado', 'siguiente_responsable', 'actualizado_en'])
        item.solicitud.actualizar_estado_desde_items()
    HistorialCotizacionDepartamental.objects.create(
        cotizacion=cotizacion, antes=antes, despues=despues, motivo=motivo.strip(), actor=actor)
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor, tipo='COTIZACION_EDITADA',
        detalle=f'Cotización #{cotizacion.pk}, versión {cotizacion.version}: ${total_anterior} → ${cotizacion.total_adquisicion}. {motivo.strip()}',
    )
    return cotizacion


@transaction.atomic
def registrar_compra_realizada(item, *, fecha_compra, importe_final, numero_pedido, comprobante, actor, cotizacion_id, version):
    from .services_departamentales import generar_ordenes_departamentales

    item = ItemCompraDepartamental.objects.select_for_update().get(pk=item.pk)
    if tiene_compra_o_recepcion(item):
        raise ValidationError('Este artículo ya tiene una compra o entrega registrada. No se registró otra compra.')
    if item.estado not in ('AUTORIZADO', 'ORDENADO') or item.solicitud.estado in ('BORRADOR', 'CANCELADA', 'COMPLETADA'):
        raise ValidationError('La cotización debe estar autorizada antes de registrar la compra.')
    cotizacion = item.cotizaciones.select_for_update().filter(seleccionada=True).first()
    if not cotizacion:
        raise ValidationError('Selecciona y autoriza una cotización antes de registrar la compra.')
    if cotizacion.pk != cotizacion_id or cotizacion.version != version:
        raise ValidationError('La cotización cambió desde que abriste el formulario. Recarga y revisa la cotización actual antes de registrar la compra.')
    importe_autorizado = cotizacion.total_adquisicion.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    if importe_final > importe_autorizado:
        raise ValidationError('El importe final supera la cotización autorizada. Edita la cotización y solicita autorización antes de registrar la compra.')
    intento = IntentoCompraDepartamental.objects.select_for_update().filter(
        item=item, estado=IntentoCompraDepartamental.ESTADO_VIGENTE
    ).first()
    if intento is None:
        if item.estado != 'AUTORIZADO':
            raise ValidationError('No se encontró la orden de este artículo. Revisa su seguimiento antes de comprar.')
        generar_ordenes_departamentales([item], actor=actor)
        intento = IntentoCompraDepartamental.objects.select_for_update().get(
            item=item, estado=IntentoCompraDepartamental.ESTADO_VIGENTE
        )
    if CompraRealizadaDepartamental.objects.filter(intento=intento).exists() or RecepcionItemDepartamental.objects.filter(
        linea_orden__intento=intento
    ).exists():
        raise ValidationError('Este artículo ya tiene una compra o entrega registrada. No se registró otra compra.')
    linea = LineaOrdenCompraDepartamental.objects.filter(intento=intento).select_related("orden").first()
    if linea is None:
        raise ValidationError('No se encontró la orden de este artículo. Revisa su seguimiento antes de comprar.')
    if linea.cotizacion_id != cotizacion.pk or linea.orden.proveedor_id != cotizacion.proveedor_id:
        raise ValidationError('La orden no corresponde a la cotización seleccionada. Revisa la orden antes de comprar.')
    compra = CompraRealizadaDepartamental(
        intento=intento, item=item, cotizacion=cotizacion, fecha_compra=fecha_compra,
        importe_final=importe_final, numero_pedido=numero_pedido, comprobante=comprobante, registrado_por=actor,
    )
    compra.full_clean()
    compra.save()
    item.estado = ItemCompraDepartamental.ESTADO_COMPRADO
    item.siguiente_responsable = ItemCompraDepartamental.RESPONSABLE_COMPRAS
    item.save(update_fields=['estado', 'siguiente_responsable', 'actualizado_en'])
    CompromisoCompraDepartamental.objects.update_or_create(intento=intento, defaults={
        'item': item, 'cotizacion': cotizacion, 'monto': importe_final, 'activo': True,
        'formalizado_en': timezone.now(), 'liberado_en': None,
    })
    item.solicitud.actualizar_estado_desde_items()
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor, tipo='COMPRA_REALIZADA',
        detalle=f'Compra del {fecha_compra:%d/%m/%Y}: ${importe_final}. Pedido: {numero_pedido or "sin referencia"}. Pendiente de entrega.',
    )
    # La cola de avisos vive en la misma transacción (una compra revertida no deja
    # aviso) y el envío se despacha hasta que el commit confirma la compra.
    programar_avisos(compra)
    return compra


CAMPOS_CORRECCION_COMPRA = ('fecha_compra', 'importe_final', 'numero_pedido')


def snapshot_compra(compra):
    return {
        'fecha_compra': compra.fecha_compra.isoformat(),
        'importe_final': format(compra.importe_final, '.2f'),
        'numero_pedido': compra.numero_pedido,
        'comprobante': Path(compra.comprobante.name).name if compra.comprobante else '',
        'version': compra.version,
    }


@transaction.atomic
def corregir_compra_realizada(compra, *, datos, version, motivo, actor):
    """Corrige los datos de una compra ya pagada dejando rastro del cambio.

    No reabre la autorización de Dirección General ni toca el estado del
    artículo: el dinero ya salió y lo que se arregla es el registro. El
    compromiso sí se realinea porque alimenta el importe comprometido.
    """
    intento_id = CompraRealizadaDepartamental.objects.values_list('intento_id', flat=True).get(pk=compra.pk)
    intento = IntentoCompraDepartamental.objects.select_for_update().get(pk=intento_id)
    compra = (CompraRealizadaDepartamental.objects.select_for_update()
              .select_related('item__solicitud').get(pk=compra.pk))
    item = compra.item
    if not motivo.strip():
        raise ValidationError('Escribe el motivo de la corrección.')
    if compra.version != version:
        raise ValidationError('Otra persona corrigió esta compra. Recarga la página y revisa los datos actuales antes de guardar.')
    antes = snapshot_compra(compra)
    for name in CAMPOS_CORRECCION_COMPRA:
        if name in datos:
            setattr(compra, name, datos[name])
    if isinstance(datos.get('comprobante'), UploadedFile):
        compra.comprobante = datos['comprobante']
    if (intento.estado in (IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO,
                           IntentoCompraDepartamental.ESTADO_REEMBOLSADO)
            and intento.reembolso_solicitado is not None
            and compra.importe_final < intento.reembolso_solicitado):
        raise ValidationError('El importe final no puede ser menor que el reembolso solicitado.')
    compra.version += 1
    compra.full_clean()
    compra.save()
    despues = snapshot_compra(compra)
    if antes['importe_final'] != despues['importe_final']:
        # El compromiso refleja lo realmente pagado; la cotización queda intacta
        # como evidencia de lo que se cotizó, aunque se haya cotizado mal.
        CompromisoCompraDepartamental.objects.filter(intento=intento).update(monto=compra.importe_final)
    HistorialCompraDepartamental.objects.create(
        compra=compra, antes=antes, despues=despues, motivo=motivo.strip(), actor=actor)
    EventoCompraDepartamental.objects.create(
        solicitud=item.solicitud, item=item, actor=actor, tipo='COMPRA_CORREGIDA',
        detalle=(f'Importe ${antes["importe_final"]} → ${despues["importe_final"]}. '
                 f'{motivo.strip()} La autorización no se reabrió: la compra ya estaba pagada.'),
    )
    return compra

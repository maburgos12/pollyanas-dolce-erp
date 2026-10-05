from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.template.loader import render_to_string
from django.views.decorators.http import require_http_methods

from .access_departamentales import _puede_ver_solicitud, _puede_enviar_solicitud
from .models import ItemCompraDepartamental
from .services_destinos_documentales import actor_actual, confirmar_destino, destinos_autorizados, vinculos_visibles


@login_required
@require_http_methods(['GET','POST'])
def destinos_documentales(request, item_pk):
    actor = actor_actual(request.user)
    item = get_object_or_404(ItemCompraDepartamental.objects.select_related('solicitud__area'),pk=item_pk)
    if not _puede_ver_solicitud(actor,item.solicitud):
        raise PermissionDenied('No puedes consultar esta solicitud.')
    equipos = destinos_autorizados(actor,'ACTIVO')
    ordenes = destinos_autorizados(actor,'ORDEN').select_related('activo_ref')
    if not equipos.exists() and not ordenes.exists():
        raise PermissionDenied('Necesitas acceso a la solicitud y a sus destinos.')
    datos = request.POST if request.method=='POST' else request.GET
    tipo, destino_id = datos.get('tipo',''), datos.get('destino_id','')
    if ':' in datos.get('destino',''):
        tipo,destino_id = datos['destino'].split(':',1)
    mensaje, ok, status = '',True,200
    if request.method=='POST':
        try:
            _,creado = confirmar_destino(user=request.user,item_id=item.pk,tipo=tipo,destino_id=destino_id,
                                        motivo=datos.get('motivo'),evidencia=datos.get('evidencia'),confirmado=datos.get('confirmado')=='on')
            mensaje = 'Destino documental confirmado.' if creado else 'Este destino ya estaba confirmado. Se conservó el registro original.'
            status = 201 if creado else 200
        except ValidationError as exc:
            mensaje,ok = ' '.join(exc.messages),False
            status = 409 if exc.code=='conflict' else 400
    puede_compra = _puede_enviar_solicitud(actor,item.solicitud)
    equipos_escritura = destinos_autorizados(actor,'ACTIVO',escritura=True) if puede_compra else equipos.none()
    ordenes_escritura = destinos_autorizados(actor,'ORDEN',escritura=True).select_related('activo_ref') if puede_compra else ordenes.none()
    contexto = {'item':item,'equipos':equipos_escritura,'ordenes':ordenes_escritura,
                'puede_crear':equipos_escritura.exists() or ordenes_escritura.exists(),
                'vinculos':vinculos_visibles(actor,item=item),'mensaje':mensaje,'ok':ok,
                'tipo_elegido':tipo,'id_elegido':str(destino_id),'motivo':datos.get('motivo',''),
                'evidencia':datos.get('evidencia',''),'confirmado':datos.get('confirmado')=='on'}
    if request.method=='POST' and ('application/json' in request.headers.get('Accept','') or request.headers.get('X-Requested-With')=='XMLHttpRequest'):
        return JsonResponse({'ok':ok,'toast':{'type':'success' if ok else 'error','message':mensaje},
                             'target':'#destinos-documentales','html':render_to_string('compras/departamentales/_destinos_documentales.html',contexto,request=request)},status=status)
    return render(request,'compras/departamentales/destinos_documentales.html',contexto,status=status)


@login_required
@require_http_methods(['GET'])
def compras_del_destino(request, tipo, destino_id):
    actor = actor_actual(request.user)
    if tipo not in ('ACTIVO','ORDEN'):
        raise PermissionDenied
    destino = get_object_or_404(destinos_autorizados(actor,tipo),pk=destino_id)
    vinculos = vinculos_visibles(actor,tipo=tipo,destino_id=destino.pk)
    if not vinculos:
        # No confirma existencia de artículos fuera del alcance de la compra.
        raise PermissionDenied('No hay relaciones de compra consultables con tus permisos actuales.')
    return render(request,'compras/departamentales/compras_del_destino.html',{'destino':destino,'tipo':tipo,'vinculos':vinculos})

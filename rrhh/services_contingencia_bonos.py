"""Proyección de bonos en memoria: reutiliza sus fórmulas sin guardar modelos."""
from copy import copy
from datetime import date, timedelta
from decimal import Decimal
from .models import BeneficiarioContingencia, IncidenciaAsistencia, PermisoContingencia
from .services_asistencia_contexto import cargar_contexto_asistencia, CODIGO_CONTINGENCIA, CODIGO_ASISTENCIA, CODIGO_RETARDO
from .services_cobertura_permisos import tiene_actividad_real, fecha_en_intervalo_laboral

CAMPOS = ('pasa_asistencia','pasa_uniforme','pasa_puntualidad','cancela_bono','total_a_pagar',
          'dias_trabajados','dias_asistencia','dias_uniforme','dias_puntualidad',
          'bono_extra','ajuste_positivo','ajuste_negativo')


def _estado(bono):
    return {campo:str(getattr(bono,campo)) if isinstance(getattr(bono,campo),Decimal) else getattr(bono,campo) for campo in CAMPOS}


def proyectar_bonos(datos, poblacion, permiso, accion, grupos, faltas_plan, alertas_plan):
    from django.apps import apps
    from django.utils import timezone
    resultado = []
    bporid = {b['empleado_id']:b for b in poblacion}
    for grupo in grupos:
        model = apps.get_model(grupo['modelo'])
        for original in model.objects.filter(pk__in=[r['id'] for r in grupo['filas']]).select_related('empleado','periodo'):
            if original.estatus!='BORRADOR' or accion=='borrador':
                continue
            bono = copy(original)
            cfg = bono.periodo
            inicio,fin = cfg.rango_fechas()
            lote = cargar_contexto_asistencia(empleados=[bono.empleado],fecha_inicio=inicio,fecha_fin=fin)
            # Instantánea hipotética: conserva otros permisos, retira solo el revocado.
            lote.contingencias = {key:[b for b in items if not (accion=='revocar' and b.permiso_id==getattr(permiso,'pk',None))] for key,items in lote.contingencias.items()}
            b = bporid[bono.empleado_id]
            fechas = []
            cursor = max(inicio,date.fromisoformat(str(b['fecha_inicio'])))
            limite = min(fin,date.fromisoformat(str(b['fecha_fin'])),timezone.localdate())
            fuente = permiso or PermisoContingencia(clave=datos['clave'])
            while cursor<=limite:
                if not fecha_en_intervalo_laboral(bono.empleado,cursor):
                    cursor+=timedelta(days=1)
                    continue
                fechas.append(cursor)
                if accion=='aprobar':
                    lote.contingencias.setdefault((bono.empleado_id,cursor),[]).append(BeneficiarioContingencia(permiso=fuente,empleado=bono.empleado,fecha_inicio=cursor,fecha_fin=cursor))
                cursor+=timedelta(days=1)
            if not fechas:
                resultado.append(dict(modelo=grupo['modelo'],id=bono.pk,antes=_estado(original),despues=_estado(original),delta_total='0.00'))
                continue
            for cambio in faltas_plan:
                if cambio['empleado_id']!=bono.empleado_id:
                    continue
                dia = date.fromisoformat(cambio['fecha'])
                key = (bono.empleado_id,dia)
                registro = next((i for i in lote.incidencias.get(key,[]) if i.tipo=='falta'),None)
                if registro:
                    registro.estado=cambio['despues']
                elif inicio<=dia<=fin:
                    lote.incidencias.setdefault(key,[]).append(IncidenciaAsistencia(empleado=bono.empleado,fecha=dia,tipo='falta',estado=cambio['despues']))
            produccion = model._meta.app_label=='bonos_produccion'
            registros = {}
            for registro in original.registros.all():
                if produccion:
                    from bonos_produccion.services_checador import _fecha_registro_legacy
                    dia = registro.fecha or _fecha_registro_legacy(cfg,registro.dia)
                else:
                    from bonos_ventas.models import fecha_registro_legacy
                    dia = registro.fecha or fecha_registro_legacy(cfg,registro.dia,bool(registro.capturado_por_id))
                if dia is None or dia in registros:
                    # El sincronizador omite identidades ambiguas; no promete recálculo.
                    registros = None
                    break
                registros[dia]=copy(registro)
            if registros is None:
                resultado.append(dict(modelo=grupo['modelo'],id=bono.pk,omitido='registros_ambiguos'))
                continue
            registro_modelo = original._meta.get_field('registros').related_model
            for dia in fechas:
                if not produccion and (dia.month!=cfg.mes or dia.year!=cfg.anio):
                    continue
                registro = registros.get(dia) or registro_modelo(bono=bono,fecha=dia,dia=dia.day)
                registro.fecha = dia
                estado = lote.clasificar(bono.empleado,dia)
                if produccion:
                    registro.falta_penalizable = bool(estado.es_exigible and not registro.tiene_asistencia) if registro.capturado_por_id else estado.falta_penalizable
                    if not registro.capturado_por_id:
                        registro.tiene_asistencia=estado.codigo in {CODIGO_ASISTENCIA,CODIGO_RETARDO}
                        registro.tiene_puntualidad=estado.codigo==CODIGO_ASISTENCIA
                elif not registro.capturado_por_id:
                    # Ventas conserva su contrato histórico; solo neutral empresarial
                    # evita acreditar una fila vacía de asistencia como trabajo.
                    asistencia = lote.asistencias.get((bono.empleado_id,dia))
                    incidencias = lote.incidencias.get((bono.empleado_id,dia),[])
                    pendientes = {i.tipo for i in incidencias if i.estado=='pendiente'}
                    suspension = any(i.tipo=='suspension' and i.estado=='conciliado' for i in incidencias)
                    registro.tiene_asistencia = bool(asistencia and not suspension and 'falta' not in pendientes)
                    registro.tiene_puntualidad = bool(asistencia and not suspension and not pendientes.intersection({'uso_tolerancia','retardo','retardo_tolerancia','falta'}))
                    if estado.codigo==CODIGO_CONTINGENCIA:
                        registro.tiene_asistencia=registro.tiene_puntualidad=False
                registros[dia]=registro
            trabajados = [r for r in registros.values() if r.tiene_asistencia]
            bono.dias_trabajados=bono.dias_asistencia=len(trabajados)
            bono.dias_uniforme=sum(bool(r.tiene_uniforme) for r in trabajados)
            bono.dias_puntualidad=sum(bool(r.tiene_puntualidad) for r in trabajados)
            neutros = sum(lote.clasificar(bono.empleado,inicio+timedelta(days=n)).codigo==CODIGO_CONTINGENCIA for n in range(max(0,(min(fin,timezone.localdate())-inicio).days+1)))
            if produccion:
                from bonos_produccion.models import ConfigBonoArea
                bono.dias_produccion=sum(bool(r.tiene_produccion) for r in trabajados)
                bono.total_embetunados=sum(r.cantidad_embetunados for r in trabajados)
                esperadas = {inicio+timedelta(days=n) for n in range(max(0,(min(fin,timezone.localdate())-inicio).days+1)) if inicio+timedelta(days=n)>=bono.empleado.fecha_ingreso}
                contextuales = {dia:r for dia,r in registros.items() if r.fecha is not None}
                bono.faltas_rrhh=sum(bool(r.falta_penalizable) for r in contextuales.values()) if esperadas.issubset(contextuales) and all(r.falta_penalizable is not None for r in contextuales.values()) else None
                regla=cfg.reglas_area.filter(area=bono.area).first() or ConfigBonoArea(periodo=cfg,area=bono.area,**ConfigBonoArea.defaults_for_area(bono.area))
                bono.recalcular(dias_neutrales=neutros,regla_area=regla)
            else:
                bono.recalcular(dias_neutrales=neutros)
            resultado.append(dict(modelo=grupo['modelo'],id=bono.pk,antes=_estado(original),despues=_estado(bono),delta_total=str(bono.total_a_pagar-original.total_a_pagar)))
    return resultado

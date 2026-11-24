"""Contrato PostgreSQL: Corte siempre precede a sus filas dependientes."""
from datetime import date
from threading import Event, Thread
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import close_old_connections, connections, transaction
from django.test import TransactionTestCase
from rrhh.models import Empleado, PrenominaCorte, PrenominaEmpleadoResumen
from rrhh.services_contingencia import previsualizar_contingencia, aplicar_contingencia


class ConcurrenciaContingenciaTests(TransactionTestCase):
    def test_recalculo_y_contingencia_no_interbloquean_corte_y_resumen(self):
        actor = get_user_model().objects.create_superuser('concurrencia-cont', '', 'x')
        empleado = Empleado.objects.create(codigo='CONT-CONC',nombre='Sintético concurrencia',fecha_ingreso=date(2026,1,1))
        corte = PrenominaCorte.objects.create(fecha_inicio=date(2026,11,21),fecha_fin=date(2026,11,21),fecha_corte=date(2026,11,21))
        PrenominaEmpleadoResumen.objects.create(corte=corte,empleado=empleado)
        datos = dict(clave='CONT-CONC',fecha_inicio='2026-11-21',fecha_fin='2026-11-21',motivo='seguridad',descripcion='Prueba de bloqueos',evidencia=['test:concurrencia'],alcance='empresa',tratamiento_salarial='conservar')
        preview = previsualizar_contingencia(datos,actor)
        esperando_corte = Event()
        errores = []
        resultados = []

        def aplicar():
            close_old_connections()
            conexion = connections['default']
            def observar(execute,sql,params,many,context):
                if 'rrhh_prenominacorte' in sql and 'FOR UPDATE' in sql:
                    esperando_corte.set()
                return execute(sql,params,many,context)
            try:
                with conexion.cursor() as cursor:
                    cursor.execute("SET statement_timeout = '8s'")
                with conexion.execute_wrapper(observar):
                    aplicar_contingencia(datos,get_user_model().objects.get(pk=actor.pk),digest=preview['digest'])
                resultados.append('aplicado')
            except ValidationError:
                resultados.append('digest_obsoleto')
            except Exception as exc:
                errores.append(exc)
            finally:
                conexion.close()

        hilo = Thread(target=aplicar,daemon=True)
        try:
            with transaction.atomic():
                PrenominaCorte.objects.select_for_update().get(pk=corte.pk)
                hilo.start()
                self.assertTrue(esperando_corte.wait(5),'La segunda conexión no llegó al bloqueo de corte.')
                # Misma dependencia que recalcular_corte_prenomina, conservando
                # aquí el corte mientras se eliminan sus resúmenes anteriores.
                corte.resumenes.all().delete()
        except Exception as exc:
            errores.append(exc)
        finally:
            hilo.join(10)
        self.assertFalse(hilo.is_alive(),'La segunda conexión no terminó en el límite.')
        self.assertFalse(errores,repr(errores))
        self.assertEqual(resultados,['digest_obsoleto'])

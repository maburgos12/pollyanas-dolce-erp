import io
import json

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from core.models import AuditLog
from rrhh.models import Empleado
from rrhh.services_catalogos import etiquetas_puestos, puesto_presentacion


class PuestosCanonicosTests(TestCase):
    def test_guarda_funcion_con_etiqueta_unica_sin_mezclar_armado(self):
        for function, label in [('EMBETUNADO', 'Embetunado'), ('ARMADO', 'Armado')]:
            for raw in ['', label.upper(), label.lower()]:
                e = Empleado.objects.create(nombre='Persona', puesto=raw,
                    puesto_operativo=function, activo=True)
                e.refresh_from_db()
                self.assertEqual(e.puesto, label)
                self.assertTrue(e.activo)
                self.assertEqual(puesto_presentacion(e, etiquetas_puestos()), label)

    def test_conserva_puesto_especifico_y_no_inventa_si_no_hay_funcion(self):
        e = Empleado.objects.create(nombre='Jefa', puesto='Encargada de Producción',
            puesto_operativo='EMBETUNADO')
        self.assertEqual(e.puesto, 'Encargada de Producción')
        e = Empleado.objects.create(nombre='Sin función', puesto='')
        self.assertEqual(e.puesto, '')
        self.assertTrue(e.activo)

    def test_update_parcial_persiste_etiqueta_sin_cambiar_otro_campo(self):
        e = Empleado.objects.create(nombre='Caja', puesto_operativo='CAJAS', puesto='cajera')
        self.assertEqual(e.puesto, 'Cajas')
        e.puesto = 'EMBETUNADO'
        e.puesto_operativo = 'EMBETUNADO'
        e.save(update_fields=['puesto_operativo'])
        e.refresh_from_db()
        self.assertEqual(e.puesto, 'Embetunado')

    def test_regularizacion_revisable_auditada_idempotente_sin_reactivar(self):
        e = Empleado.objects.create(nombre='Histórica', puesto_operativo='EMBETUNADO', activo=False)
        Empleado.objects.filter(pk=e.pk).update(puesto='EMBETUNADO')
        out = io.StringIO(); call_command('regularizar_puestos_catalogo', stdout=out)
        plan = json.loads(out.getvalue())
        e.refresh_from_db(); self.assertEqual(e.puesto, 'EMBETUNADO')
        with self.assertRaises(CommandError):
            call_command('regularizar_puestos_catalogo', apply=True, expected_sha256='otro')
        call_command('regularizar_puestos_catalogo', apply=True,
            expected_sha256=plan['sha256'], stdout=io.StringIO())
        e.refresh_from_db(); self.assertEqual(e.puesto, 'Embetunado'); self.assertFalse(e.activo)
        self.assertEqual(AuditLog.objects.filter(model='rrhh.Empleado', object_id=str(e.pk)).count(), 1)
        out = io.StringIO(); call_command('regularizar_puestos_catalogo', stdout=out)
        self.assertEqual(json.loads(out.getvalue())['cambios'], [])

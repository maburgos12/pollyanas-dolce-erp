from decimal import Decimal as D
from types import SimpleNamespace as S
from django.test import SimpleTestCase
from reportes.services_costeo_logistica import repartir_centavos, distribuir_fondo, valorar_carga


class RepartoLogisticoTests(SimpleTestCase):
    def linea(self, pk, ruta, valor, sid=1, identidad='receta:7', unidad='PZA'):
        return {'id': pk, 'ruta_id': ruta, 'valor': D(valor) if valor is not None else None,
            'sucursal_id': sid, 'sucursal': str(sid), 'identidad': identidad, 'unidad': unidad,
            'nombre': identidad, 'cantidad': D(2), 'fuente': 'Histórico'}

    def test_centavos_conservados_desempate_estable_y_base_invalida(self):
        self.assertEqual(repartir_centavos(D('1'), {3:D(1), 2:D(1), 1:D(1)}),
                         {3:D('.33'),2:D('.33'),1:D('.34')})
        self.assertEqual(repartir_centavos(D(1), {1:D(0)}), {})
        for b in [D(-1), D('NaN'), D('Infinity')]:
            with self.assertRaises(ValueError):
                repartir_centavos(D(1), {1:b})

    def test_fondo_completo_km_y_valor_sin_sumar_referencia_previa(self):
        rutas = [{'id':1,'km_reparto':D(10),'nomina':D(999)}, {'id':2,'km_reparto':D(30)}]
        lineas = [self.linea(1,1,'10'), self.linea(2,1,'30',2,unidad='KG'), self.linea(3,2,'5')]
        d=distribuir_fondo(D('100.01'),rutas,lineas)
        self.assertEqual([r['monto'] for r in d['rutas']], [D('25'),D('75.01')])
        self.assertEqual(d['asignado_productos'],D('100.01'))
        self.assertEqual(d['asignado_destinos'],D('100.01'))
        self.assertEqual(d['pendiente_productos'],0)
        self.assertEqual(sum(p['monto'] for p in d['productos']),D('100.01'))
        self.assertEqual(len(d['productos']),2)

    def test_costo_desconocido_no_es_cero_ni_reparte_sobre_otros(self):
        d=distribuir_fondo(D(100),[{'id':1,'km_reparto':D(10)}],
            [self.linea(1,1,'5'),self.linea(2,1,None)])
        self.assertEqual(d['asignado_rutas'],100)
        self.assertEqual(d['asignado_destinos'],100)
        self.assertEqual(d['asignado_productos'],0)
        self.assertEqual(d['pendiente_productos'],100)
        self.assertEqual(len(d['sin_valor']),1)

    def test_distancia_ausente_no_absorbe_fondo_en_otras_rutas(self):
        d=distribuir_fondo(D(100),[{'id':1,'km_reparto':D(0)},{'id':2,'km_reparto':D(10)}],
            [self.linea(1,1,'5'),self.linea(2,2,'10')])
        self.assertEqual(d['asignado_rutas'],0)
        self.assertEqual(d['pendiente_productos'],100)

    def test_variantes_no_se_fusionan_por_nombre(self):
        d=distribuir_fondo(D(100),[{'id':1,'km_reparto':D(10)}],
            [self.linea(1,1,'5',identidad='articulo:10'),self.linea(2,1,'5',identidad='articulo:11')])
        self.assertEqual(len(d['productos']),2)

    def test_costo_cero_transferencia_usa_receta_del_periodo(self):
        linea=S(unit='PZA',point_transfer_line=S(pk=1,is_insumo=False,is_cancelled=False,unit='PZA',unit_cost=D(0),receta_id=7,raw_payload={}))
        receta=S(pk=9,receta=S(rendimiento_unidad=None,rendimiento_cantidad=D(1)),costo_por_unidad_rendimiento=D(12),costo_total=D(12))
        self.assertEqual(valorar_carga(linea,{7:receta},{}),(D(12),'Receta mensual #9'))
        linea.unit='KG'
        self.assertIsNone(valorar_carga(linea,{7:receta},{})[0])

    def test_reventa_identidad_externa_no_sku(self):
        linea=S(unit='PZA',point_transfer_line=S(pk=1,is_insumo=False,is_cancelled=False,unit='PZA',unit_cost=D(0),receta_id=None,raw_payload={'detail':{'FK_articulo':1056}}))
        otro=S(pk=4,costo_promedio=D(40))
        self.assertIsNone(valorar_carga(linea,{}, {'1055':otro})[0])
        self.assertEqual(valorar_carga(linea,{}, {'1056':otro})[0],D(40))

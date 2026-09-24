from calendar import monthrange
from datetime import UTC, date, datetime
from decimal import Decimal as D
from io import StringIO

from django.core.management import call_command
from django.db import connection
from django.db import IntegrityError, transaction
from django.db.models.deletion import ProtectedError
from django.test import SimpleTestCase, TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from core.models import Sucursal
from reportes.models import DistribucionISNEmpleado, ExpedienteISN
from reportes.services_isn import (
    MAX_CFDI_XML_BYTES,
    _validar_lineas_nomina_mes,
    bases_gravadas_empleados,
    calcular_isn_sinaloa,
    extraer_isn_cfdi,
    aplicar_expediente_isn,
    preparar_expediente_isn,
    prorratear_isn,
)
from rrhh.models import (
    Empleado,
    NominaConceptoLinea,
    NominaLinea,
    NominaPeriodo,
)
from sat_client.models import CfdiDescargado


class ISNMathTests(SimpleTestCase):
    def test_tarifa_progresiva_sinaloa(self):
        self.assertEqual(calcular_isn_sinaloa(D("500000.00")), D("12000.00"))
        self.assertEqual(calcular_isn_sinaloa(D("660307.70")), D("16168.00"))
        self.assertEqual(calcular_isn_sinaloa(D("707079.82")), D("17398.23"))
        self.assertEqual(calcular_isn_sinaloa(D("1000000.00")), D("25800.00"))

    def test_prorrateo_cierra_exactamente_a_centavos(self):
        result = prorratear_isn({3: D("1"), 1: D("1"), 2: D("1")}, D("10.00"))

        self.assertEqual(sum(result.values(), D("0")), D("10.00"))
        self.assertEqual(result, {1: D("3.34"), 2: D("3.33"), 3: D("3.33")})


class ISNSourceTests(TestCase):
    CFDI_XML = """<?xml version="1.0" encoding="UTF-8"?>
    <cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4">
      <cfdi:Conceptos>
        <cfdi:Concepto NoIdentificacion="OTRO" Descripcion="Otro cobro" Importe="999.99" />
        <cfdi:Concepto NoIdentificacion=" 202608   2-003 " Descripcion="Servicio estatal" Importe="16168.00" />
      </cfdi:Conceptos>
    </cfdi:Comprobante>
    """

    def _crear_cfdi(self, **overrides):
        datos = {
            "uuid": "CFDI-ISN-FUENTE",
            "rfc_emisor": " ges8101015i7 ",
            "rfc_receptor": " gef211230kr2 ",
            "subtotal": D("16168.00"),
            "total": D("16168.00"),
            "tipo_comprobante": "I",
            "tipo_cfdi": CfdiDescargado.TIPO_RECIBIDO,
            "fecha_emision": datetime(2026, 9, 17, tzinfo=UTC),
            "estatus": " VIGENTE ",
            "xml_raw": self.CFDI_XML,
        }
        datos.update(overrides)
        return CfdiDescargado.objects.create(**datos)

    def _crear_empleado(self, codigo="E-ISN", **overrides):
        sucursal = overrides.pop("sucursal_ref", None)
        if sucursal is None:
            sucursal = Sucursal.objects.create(
                codigo=f"S-{codigo}",
                nombre=f"Sucursal {codigo}",
            )
        datos = {
            "codigo": codigo,
            "nombre": f"Empleado {codigo}",
            "departamento": Empleado.DEP_VENTAS,
            "sucursal_ref": sucursal,
        }
        datos.update(overrides)
        return Empleado.objects.create(**datos)

    def _crear_periodo(
        self,
        *,
        empleado,
        estatus,
        fecha_fin,
        conceptos,
        fecha_inicio=None,
        tipo_periodo=NominaPeriodo.TIPO_QUINCENAL,
    ):
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=fecha_inicio or fecha_fin.replace(day=1),
            fecha_fin=fecha_fin,
            estatus=estatus,
            tipo_periodo=tipo_periodo,
        )
        linea = NominaLinea.objects.create(periodo=periodo, empleado=empleado)
        for codigo, importe in conceptos:
            NominaConceptoLinea.objects.create(
                linea=linea,
                tipo=NominaConceptoLinea.TIPO_PERCEPCION,
                codigo_concepto=codigo,
                nombre=f"Concepto {codigo}",
                importe=importe,
            )
        return periodo

    def _crear_mes_valido(
        self,
        *,
        empleado,
        anio,
        mes,
        conceptos_primera=(),
        conceptos_segunda=(),
    ):
        ultimo_dia = monthrange(anio, mes)[1]
        primera = self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_CERRADA,
            fecha_inicio=date(anio, mes, 1),
            fecha_fin=date(anio, mes, 15),
            conceptos=conceptos_primera,
        )
        segunda = self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_PAGADA,
            fecha_inicio=date(anio, mes, 16),
            fecha_fin=date(anio, mes, ultimo_dia),
            conceptos=conceptos_segunda,
        )
        return primera, segunda

    def test_extrae_periodo_del_concepto_y_no_de_fecha_emision(self):
        cfdi = self._crear_cfdi()

        periodo, importe = extraer_isn_cfdi(cfdi)

        self.assertEqual(periodo, date(2026, 8, 1))
        self.assertEqual(importe, D("16168.00"))

    def test_rechaza_cfdi_que_no_es_ingreso_recibido_vigente_de_la_empresa(self):
        casos = (
            ("rfc_emisor", "AAA010101AAA"),
            ("rfc_receptor", "BBB010101BBB"),
            ("estatus", "cancelado"),
            ("tipo_cfdi", CfdiDescargado.TIPO_EMITIDO),
            ("tipo_comprobante", "E"),
        )
        for indice, (campo, valor) in enumerate(casos, start=1):
            with self.subTest(campo=campo):
                cfdi = self._crear_cfdi(uuid=f"CFDI-INVALIDO-{indice}", **{campo: valor})
                with self.assertRaises(ValueError):
                    extraer_isn_cfdi(cfdi)

    def test_rechaza_xml_sin_periodo_o_con_periodos_ambiguos(self):
        xml_sin_periodo = self.CFDI_XML.replace("202608   2-003", "SIN-PERIODO")
        xml_ambiguo = self.CFDI_XML.replace(
            "</cfdi:Conceptos>",
            '<cfdi:Concepto NoIdentificacion="202607 2-003" '
            'Descripcion="Impuesto sobre nomina" Importe="15000.00" />'
            "</cfdi:Conceptos>",
        )
        for indice, xml in enumerate(("<xml", xml_sin_periodo, xml_ambiguo), start=1):
            with self.subTest(indice=indice):
                cfdi = self._crear_cfdi(uuid=f"CFDI-XML-{indice}", xml_raw=xml)
                with self.assertRaises(ValueError):
                    extraer_isn_cfdi(cfdi)

    def test_suma_multiples_conceptos_canonicos_del_mismo_periodo(self):
        xml = self.CFDI_XML.replace(
            "</cfdi:Conceptos>",
            '<cfdi:Concepto NoIdentificacion="202608 2-003" '
            'Descripcion="Segundo" Importe="1.00" />'
            "</cfdi:Conceptos>",
        )
        cfdi = self._crear_cfdi(uuid="CFDI-MULTIPLES", xml_raw=xml)

        periodo, importe = extraer_isn_cfdi(cfdi)

        self.assertEqual(periodo, date(2026, 8, 1))
        self.assertEqual(importe, D("16169.00"))

    def test_acepta_legacy_aaaamm_con_descripcion_nomina(self):
        xml = self.CFDI_XML.replace(" 202608   2-003 ", "202608").replace(
            "Servicio estatal",
            "Impuesto sobre nomina",
        )
        cfdi = self._crear_cfdi(uuid="CFDI-LEGACY", xml_raw=xml)

        periodo, importe = extraer_isn_cfdi(cfdi)

        self.assertEqual(periodo, date(2026, 8, 1))
        self.assertEqual(importe, D("16168.00"))

    def test_descripcion_legacy_normaliza_acento(self):
        xml = self.CFDI_XML.replace(" 202608   2-003 ", "202608").replace(
            "Servicio estatal",
            "Impuesto Sobre NÓMINA",
        )
        cfdi = self._crear_cfdi(uuid="CFDI-LEGACY-ACENTO", xml_raw=xml)

        periodo, importe = extraer_isn_cfdi(cfdi)

        self.assertEqual(periodo, date(2026, 8, 1))
        self.assertEqual(importe, D("16168.00"))

    def test_rechaza_codigo_distinto_con_descripcion_generica(self):
        xml = self.CFDI_XML.replace("202608   2-003", "202608 9-999")
        cfdi = self._crear_cfdi(uuid="CFDI-CODIGO-DISTINTO", xml_raw=xml)

        with self.assertRaises(ValueError):
            extraer_isn_cfdi(cfdi)

    def test_rechaza_mezcla_con_codigo_fiscal_no_clasificable(self):
        xml = self.CFDI_XML.replace(
            "</cfdi:Conceptos>",
            '<cfdi:Concepto NoIdentificacion="202608 9-999" '
            'Descripcion="Impuesto sobre nomina" Importe="1.00" />'
            "</cfdi:Conceptos>",
        )
        cfdi = self._crear_cfdi(uuid="CFDI-MEZCLA", xml_raw=xml)

        with self.assertRaises(ValueError):
            extraer_isn_cfdi(cfdi)

    def test_rechaza_importe_no_positivo_o_no_finito(self):
        for indice, importe in enumerate(("0", "-0.01", "NaN", "Infinity"), start=1):
            with self.subTest(importe=importe):
                xml = self.CFDI_XML.replace('Importe="16168.00"', f'Importe="{importe}"')
                cfdi = self._crear_cfdi(uuid=f"CFDI-IMPORTE-{indice}", xml_raw=xml)
                with self.assertRaises(ValueError):
                    extraer_isn_cfdi(cfdi)

    def test_rechaza_xml_vacio_excesivo_o_con_declaraciones_peligrosas(self):
        casos = (
            "",
            "x" * (MAX_CFDI_XML_BYTES + 1),
            '<!DOCTYPE foo SYSTEM "externo"><foo />',
            '<!ENTITY xxe SYSTEM "externo"><foo />',
        )
        for indice, xml in enumerate(casos, start=1):
            with self.subTest(indice=indice):
                cfdi = self._crear_cfdi(uuid=f"CFDI-SEGURO-{indice}", xml_raw=xml)
                with self.assertRaises(ValueError):
                    extraer_isn_cfdi(cfdi)

    def test_calcula_base_por_empleado_con_exenciones_de_codigos_y_aguinaldo(self):
        empleado = self._crear_empleado()
        self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=8,
            conceptos_primera=(
                ("1", D("10000.00")),
                ("20", D("1000.00")),
                ("22", D("2000.00")),
                ("24", D("4000.00")),
                ("26", D("300.00")),
                ("32", D("400.00")),
            ),
        )

        bases = bases_gravadas_empleados(date(2026, 8, 1))

        self.assertEqual(bases, {empleado.id: D("10480.70")})

    def test_aguinaldo_consume_tope_anual_sin_arrastrar_salario_previo(self):
        empleado = self._crear_empleado(codigo="E-AGUINALDO-YTD")
        self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=1,
            conceptos_primera=(("1", D("9000.00")), (" 24 ", D("2000.00"))),
        )
        self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=8,
            conceptos_primera=(("24", D("2000.00")),),
        )

        bases = bases_gravadas_empleados(date(2026, 8, 1))

        self.assertEqual(bases, {empleado.id: D("480.70")})

    def test_agrupa_por_empleado_solo_periodos_cerrados_o_pagados_que_terminan_en_mes(self):
        empleado = self._crear_empleado(codigo="E-AGRUPADO")
        casos = (
            (NominaPeriodo.ESTATUS_CERRADA, date(2026, 8, 7), D("100.00")),
            (NominaPeriodo.ESTATUS_PAGADA, date(2026, 8, 31), D("200.00")),
            (NominaPeriodo.ESTATUS_BORRADOR, date(2026, 8, 15), D("400.00")),
            (NominaPeriodo.ESTATUS_CERRADA, date(2026, 9, 7), D("800.00")),
        )
        inicios = (date(2026, 8, 1), date(2026, 8, 8), date(2026, 8, 16), date(2026, 9, 1))
        for (estatus, fecha_fin, importe), fecha_inicio in zip(casos, inicios):
            self._crear_periodo(
                empleado=empleado,
                estatus=estatus,
                fecha_fin=fecha_fin,
                conceptos=(("1", importe),),
                fecha_inicio=fecha_inicio,
            )

        bases = bases_gravadas_empleados(date(2026, 8, 1))

        self.assertEqual(bases, {empleado.id: D("300.00")})

    def test_rechaza_anio_sin_uma_configurada(self):
        with self.assertRaisesMessage(ValueError, "UMA"):
            bases_gravadas_empleados(date(2027, 1, 1))

    def test_uma_cambia_en_la_frontera_de_febrero_2026(self):
        empleado_enero = self._crear_empleado(codigo="E-UMA-ENERO")
        self._crear_mes_valido(
            empleado=empleado_enero,
            anio=2026,
            mes=1,
            conceptos_primera=(("24", D("3400.00")),),
        )
        empleado_febrero = self._crear_empleado(codigo="E-UMA-FEBRERO")
        self._crear_mes_valido(
            empleado=empleado_febrero,
            anio=2026,
            mes=2,
            conceptos_primera=(("24", D("3520.00")),),
        )

        self.assertEqual(
            bases_gravadas_empleados(date(2026, 1, 31)),
            {empleado_enero.id: D("5.80")},
        )
        self.assertEqual(
            bases_gravadas_empleados(date(2026, 2, 1)),
            {empleado_febrero.id: D("0.70")},
        )

    def test_rechaza_mes_sin_periodos_cerrados_o_pagados(self):
        empleado = self._crear_empleado(codigo="E-SIN-VALIDO")
        self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_BORRADOR,
            fecha_fin=date(2026, 8, 15),
            conceptos=(("1", D("100.00")),),
        )

        with self.assertRaisesMessage(ValueError, "periodos"):
            bases_gravadas_empleados(date(2026, 8, 1))

    def test_rechaza_mes_con_una_sola_quincena(self):
        empleado = self._crear_empleado(codigo="E-UNA-QUINCENA")
        self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_CERRADA,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 15),
            conceptos=(("1", D("100.00")),),
        )

        with self.assertRaisesMessage(ValueError, "dos periodos"):
            bases_gravadas_empleados(date(2026, 8, 1))

    def test_rechaza_quincena_vacia(self):
        empleado = self._crear_empleado(codigo="E-QUINCENA-VACIA")
        self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_CERRADA,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 15),
            conceptos=(),
        )
        NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 8, 16),
            fecha_fin=date(2026, 8, 31),
            estatus=NominaPeriodo.ESTATUS_PAGADA,
            tipo_periodo=NominaPeriodo.TIPO_QUINCENAL,
        )

        with self.assertRaisesMessage(ValueError, "vacia"):
            bases_gravadas_empleados(date(2026, 8, 1))

    def test_rechaza_hueco_y_rango_invertido(self):
        casos = (
            ((1, 14), (16, 31), "hueco"),
            ((1, 15), (20, 16), "invertido"),
        )
        for indice, (primera, segunda, mensaje) in enumerate(casos, start=1):
            with self.subTest(mensaje=mensaje):
                empleado = self._crear_empleado(codigo=f"E-RANGO-{indice}")
                self._crear_periodo(
                    empleado=empleado,
                    estatus=NominaPeriodo.ESTATUS_CERRADA,
                    fecha_inicio=date(2026, 8, primera[0]),
                    fecha_fin=date(2026, 8, primera[1]),
                    conceptos=(),
                )
                self._crear_periodo(
                    empleado=empleado,
                    estatus=NominaPeriodo.ESTATUS_PAGADA,
                    fecha_inicio=date(2026, 8, segunda[0]),
                    fecha_fin=date(2026, 8, segunda[1]),
                    conceptos=(),
                )
                with self.assertRaisesMessage(ValueError, mensaje):
                    bases_gravadas_empleados(date(2026, 8, 1))
                NominaPeriodo.objects.all().delete()

    def test_rechaza_cobertura_incompleta_del_mes(self):
        empleado = self._crear_empleado(codigo="E-COBERTURA")
        self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_CERRADA,
            fecha_inicio=date(2026, 8, 2),
            fecha_fin=date(2026, 8, 15),
            conceptos=(),
        )
        self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_PAGADA,
            fecha_inicio=date(2026, 8, 16),
            fecha_fin=date(2026, 8, 31),
            conceptos=(),
        )

        with self.assertRaisesMessage(ValueError, "cobertura completa"):
            bases_gravadas_empleados(date(2026, 8, 1))

    def test_rechaza_mes_historico_de_aguinaldo_con_rangos_invalidos(self):
        for indice, rangos in enumerate(
            (
                ((1, 15), (1, 15)),
                ((1, 20), (15, 31)),
            ),
            start=1,
        ):
            with self.subTest(indice=indice):
                empleado = self._crear_empleado(codigo=f"E-HIST-{indice}")
                for numero, (inicio, fin) in enumerate(rangos):
                    self._crear_periodo(
                        empleado=empleado,
                        estatus=(
                            NominaPeriodo.ESTATUS_CERRADA
                            if numero == 0
                            else NominaPeriodo.ESTATUS_PAGADA
                        ),
                        fecha_inicio=date(2026, 1, inicio),
                        fecha_fin=date(2026, 1, fin),
                        conceptos=((" 24 ", D("2000.00")),) if numero == 0 else (),
                    )
                self._crear_mes_valido(
                    empleado=empleado,
                    anio=2026,
                    mes=8,
                    conceptos_primera=(("24", D("2000.00")),),
                )

                with self.assertRaises(ValueError):
                    bases_gravadas_empleados(date(2026, 8, 1))
                NominaPeriodo.objects.all().delete()

    def test_numero_de_consultas_no_crece_con_los_conceptos(self):
        empleado = self._crear_empleado(codigo="E-QUERIES")
        primera, _ = self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=8,
            conceptos_primera=(("1", D("100.00")),),
        )
        linea = primera.lineas.get(empleado=empleado)

        with CaptureQueriesContext(connection) as pocas:
            bases_gravadas_empleados(date(2026, 8, 1))
        NominaConceptoLinea.objects.bulk_create(
            [
                NominaConceptoLinea(
                    linea=linea,
                    tipo=NominaConceptoLinea.TIPO_PERCEPCION,
                    codigo_concepto="20",
                    nombre="Exento",
                    importe=D("1.00"),
                )
                for _ in range(25)
            ]
        )
        with CaptureQueriesContext(connection) as muchas:
            bases_gravadas_empleados(date(2026, 8, 1))

        self.assertEqual(len(pocas), len(muchas))
        self.assertLessEqual(len(muchas), 6)

    def test_rechaza_periodos_con_rango_duplicado_o_solapado(self):
        empleado = self._crear_empleado(codigo="E-RANGOS")
        periodo = self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_CERRADA,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 15),
            conceptos=(("1", D("100.00")),),
        )
        for indice, (inicio, fin) in enumerate(
            ((date(2026, 8, 1), date(2026, 8, 15)), (date(2026, 8, 15), date(2026, 8, 31))),
            start=1,
        ):
            with self.subTest(indice=indice):
                segundo = NominaPeriodo.objects.create(
                    fecha_inicio=inicio,
                    fecha_fin=fin,
                    estatus=NominaPeriodo.ESTATUS_PAGADA,
                )
                NominaLinea.objects.create(periodo=segundo, empleado=empleado)
                with self.assertRaises(ValueError):
                    bases_gravadas_empleados(date(2026, 8, 1))
                segundo.delete()
        periodo.delete()

    def test_rechaza_mezcla_de_tipo_periodo_en_el_mes(self):
        empleado = self._crear_empleado(codigo="E-TIPOS")
        self._crear_periodo(
            empleado=empleado,
            estatus=NominaPeriodo.ESTATUS_CERRADA,
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 15),
            conceptos=(("1", D("100.00")),),
            tipo_periodo=NominaPeriodo.TIPO_QUINCENAL,
        )
        NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 8, 16),
            fecha_fin=date(2026, 8, 31),
            estatus=NominaPeriodo.ESTATUS_PAGADA,
            tipo_periodo=NominaPeriodo.TIPO_SEMANAL,
        )

        with self.assertRaisesMessage(ValueError, "tipo"):
            bases_gravadas_empleados(date(2026, 8, 1))

    def test_incluye_empleado_sin_percepciones_con_base_cero(self):
        empleado = self._crear_empleado(codigo="E-SIN-PERCEPCIONES")
        self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=8,
        )

        self.assertEqual(
            bases_gravadas_empleados(date(2026, 8, 1)),
            {empleado.id: D("0.00")},
        )

    def test_rechaza_empleado_sin_metadatos_aunque_no_tenga_percepciones(self):
        empleado = self._crear_empleado(codigo="E-CERO-SIN-DEP", departamento="")
        self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=8,
        )

        with self.assertRaises(ValueError):
            bases_gravadas_empleados(date(2026, 8, 1))

    def test_rechaza_percepcion_negativa(self):
        empleado = self._crear_empleado(codigo="E-NEGATIVO")
        self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=8,
            conceptos_primera=(("1", D("-0.01")),),
        )

        with self.assertRaisesMessage(ValueError, "negativa"):
            bases_gravadas_empleados(date(2026, 8, 1))

    def test_rechaza_linea_duplicada_por_periodo_y_empleado(self):
        linea_a = type("Linea", (), {"periodo_id": 1, "empleado_id": 7})()
        linea_b = type("Linea", (), {"periodo_id": 1, "empleado_id": 7})()

        with self.assertRaisesMessage(ValueError, "duplicada"):
            _validar_lineas_nomina_mes([linea_a, linea_b])

    def test_rechaza_empleado_sin_sucursal(self):
        empleado = self._crear_empleado(codigo="E-SIN-SUCURSAL")
        empleado.sucursal_ref = None
        empleado.save(update_fields={"sucursal_ref"})
        self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=8,
            conceptos_primera=(("1", D("100.00")),),
        )

        with self.assertRaises(ValueError):
            bases_gravadas_empleados(date(2026, 8, 1))

    def test_rechaza_empleado_sin_departamento(self):
        empleado = self._crear_empleado(
            codigo="E-SIN-DEPARTAMENTO",
            departamento="",
        )
        self._crear_mes_valido(
            empleado=empleado,
            anio=2026,
            mes=8,
            conceptos_primera=(("1", D("100.00")),),
        )

        with self.assertRaises(ValueError):
            bases_gravadas_empleados(date(2026, 8, 1))


class ISNApplicationTests(TestCase):
    PERIODO = date(2026, 8, 1)

    def _crear_cfdi(self, uuid, importe="100.00"):
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4">
          <cfdi:Conceptos>
            <cfdi:Concepto NoIdentificacion="202608 2-003"
              Descripcion="Impuesto sobre nomina" Importe="{importe}" />
          </cfdi:Conceptos>
        </cfdi:Comprobante>
        """
        return CfdiDescargado.objects.create(
            uuid=uuid,
            rfc_emisor="GES8101015I7",
            rfc_receptor="GEF211230KR2",
            subtotal=D(importe),
            total=D(importe),
            tipo_comprobante="I",
            tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
            fecha_emision=datetime(2026, 9, 17, tzinfo=UTC),
            estatus="VIGENTE",
            xml_raw=xml,
        )

    def _crear_empleado(self, codigo, departamento=Empleado.DEP_VENTAS):
        sucursal = Sucursal.objects.create(
            codigo=f"S-{codigo}",
            nombre=f"Sucursal {codigo}",
        )
        return Empleado.objects.create(
            codigo=codigo,
            nombre=f"Empleado {codigo}",
            departamento=departamento,
            sucursal_ref=sucursal,
        )

    def _crear_nomina_completa(self, bases):
        periodos = (
            NominaPeriodo.objects.create(
                fecha_inicio=date(2026, 8, 1),
                fecha_fin=date(2026, 8, 15),
                estatus=NominaPeriodo.ESTATUS_CERRADA,
                tipo_periodo=NominaPeriodo.TIPO_QUINCENAL,
            ),
            NominaPeriodo.objects.create(
                fecha_inicio=date(2026, 8, 16),
                fecha_fin=date(2026, 8, 31),
                estatus=NominaPeriodo.ESTATUS_PAGADA,
                tipo_periodo=NominaPeriodo.TIPO_QUINCENAL,
            ),
        )
        for empleado, base in bases:
            for indice, periodo in enumerate(periodos):
                linea = NominaLinea.objects.create(
                    periodo=periodo,
                    empleado=empleado,
                )
                if indice == 0 and base:
                    NominaConceptoLinea.objects.create(
                        linea=linea,
                        tipo=NominaConceptoLinea.TIPO_PERCEPCION,
                        codigo_concepto="1",
                        nombre="Sueldo",
                        importe=base,
                    )

    def test_dry_run_no_escribe_y_muestra_preview(self):
        empleado = self._crear_empleado("E-DRY")
        self._crear_nomina_completa(((empleado, D("4000.00")),))
        cfdi = self._crear_cfdi("CFDI-ISN-DRY")
        stdout = StringIO()

        call_command(
            "materializar_isn",
            periodo="2026-08",
            uuid=cfdi.uuid,
            stdout=stdout,
        )

        self.assertEqual(ExpedienteISN.objects.count(), 0)
        self.assertEqual(DistribucionISNEmpleado.objects.count(), 0)
        self.assertIn("CFDI-ISN-DRY", stdout.getvalue())
        self.assertIn("DRY-RUN: sin cambios", stdout.getvalue())

    def test_apply_crea_expediente_y_linea_por_empleado_incluso_base_cero(self):
        empleado_a = self._crear_empleado("E-APPLY-A")
        empleado_b = self._crear_empleado("E-APPLY-B", Empleado.DEP_PRODUCCION)
        self._crear_nomina_completa(
            ((empleado_a, D("4000.00")), (empleado_b, D("0.00")))
        )
        cfdi = self._crear_cfdi("CFDI-ISN-APPLY")

        preview = preparar_expediente_isn(self.PERIODO, uuid=cfdi.uuid)
        expediente = aplicar_expediente_isn(
            preview,
            base_declarada=D("4000.00"),
        )

        self.assertEqual(expediente.estado, ExpedienteISN.ESTADO_APLICADO)
        self.assertEqual(expediente.revision, 1)
        self.assertEqual(expediente.base_declarada, D("4000.00"))
        lineas = list(expediente.distribuciones.order_by("empleado_id"))
        self.assertEqual(len(lineas), 2)
        self.assertEqual(
            sum((linea.base_gravada for linea in lineas), D("0")),
            D("4000.00"),
        )
        self.assertEqual(
            sum((linea.monto_isn for linea in lineas), D("0")),
            D("100.00"),
        )
        linea_cero = next(
            linea for linea in lineas if linea.empleado_id == empleado_b.id
        )
        self.assertEqual(linea_cero.base_gravada, D("0.00"))
        self.assertEqual(linea_cero.monto_isn, D("0.00"))
        self.assertEqual(linea_cero.area_codigo, Empleado.DEP_PRODUCCION)
        self.assertEqual(linea_cero.sucursal_id, empleado_b.sucursal_ref_id)

    def test_reaplicar_mismo_uuid_es_idempotente(self):
        empleado = self._crear_empleado("E-IDEMPOTENTE")
        self._crear_nomina_completa(((empleado, D("4000.00")),))
        cfdi = self._crear_cfdi("CFDI-ISN-IDEMPOTENTE")
        preview = preparar_expediente_isn(self.PERIODO, uuid=cfdi.uuid)

        primero = aplicar_expediente_isn(preview)
        segundo = aplicar_expediente_isn(preview)

        self.assertEqual(segundo.pk, primero.pk)
        self.assertEqual(ExpedienteISN.objects.count(), 1)
        self.assertEqual(DistribucionISNEmpleado.objects.count(), 1)

    def test_cfdi_correctivo_crea_revision_y_reemplaza_sin_borrar_aplicado_en(self):
        empleado = self._crear_empleado("E-CORRECTIVO")
        self._crear_nomina_completa(((empleado, D("4000.00")),))
        cfdi_original = self._crear_cfdi("CFDI-ISN-ORIGINAL", "100.00")
        original = aplicar_expediente_isn(
            preparar_expediente_isn(self.PERIODO, uuid=cfdi_original.uuid)
        )
        aplicado_en_original = original.aplicado_en
        cfdi_correctivo = self._crear_cfdi("CFDI-ISN-CORRECTIVO", "110.00")

        correctivo = aplicar_expediente_isn(
            preparar_expediente_isn(self.PERIODO, uuid=cfdi_correctivo.uuid)
        )

        original.refresh_from_db()
        self.assertEqual(original.estado, ExpedienteISN.ESTADO_REEMPLAZADO)
        self.assertEqual(original.aplicado_en, aplicado_en_original)
        self.assertEqual(correctivo.revision, 2)
        self.assertEqual(correctivo.estado, ExpedienteISN.ESTADO_APLICADO)
        self.assertEqual(correctivo.importe_pagado, D("110.00"))
        self.assertEqual(ExpedienteISN.objects.count(), 2)

    def test_nomina_incompleta_falla_antes_de_escribir(self):
        empleado = self._crear_empleado("E-INCOMPLETO")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 8, 1),
            fecha_fin=date(2026, 8, 15),
            estatus=NominaPeriodo.ESTATUS_CERRADA,
            tipo_periodo=NominaPeriodo.TIPO_QUINCENAL,
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado)
        cfdi = self._crear_cfdi("CFDI-ISN-INCOMPLETO")

        with self.assertRaises(ValueError):
            preparar_expediente_isn(self.PERIODO, uuid=cfdi.uuid)

        self.assertEqual(ExpedienteISN.objects.count(), 0)
        self.assertEqual(DistribucionISNEmpleado.objects.count(), 0)


class ISNModelTests(TestCase):
    def _crear_cfdi(self, uuid):
        return CfdiDescargado.objects.create(
            uuid=uuid,
            rfc_emisor="GES8101015I7",
            rfc_receptor="GEF211230KR2",
            subtotal=D("100.00"),
            total=D("100.00"),
            tipo_comprobante="I",
            tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
            fecha_emision=timezone.now(),
        )

    def _datos_expediente(self, *, cfdi, **overrides):
        datos = {
            "periodo": date(2026, 8, 1),
            "revision": 1,
            "uuid": "A",
            "cfdi": cfdi,
            "importe_pagado": D("100.00"),
            "base_gravada_calculada": D("4000.00"),
            "estado": ExpedienteISN.ESTADO_APLICADO,
            "aplicado_en": timezone.now(),
        }
        datos.update(overrides)
        return datos

    def _crear_expediente(self, *, cfdi, **overrides):
        return ExpedienteISN.objects.create(
            **self._datos_expediente(cfdi=cfdi, **overrides)
        )

    def test_solo_hay_un_expediente_aplicado_por_periodo(self):
        self._crear_expediente(cfdi=self._crear_cfdi("CFDI-A"))

        with self.assertRaises(IntegrityError), transaction.atomic():
            self._crear_expediente(
                cfdi=self._crear_cfdi("CFDI-B"),
                uuid="B",
                revision=2,
            )

    def test_empleado_no_se_duplica_en_un_expediente(self):
        sucursal = Sucursal.objects.create(codigo="S1", nombre="Sucursal 1")
        empleado = Empleado.objects.create(codigo="E1", nombre="Empleado 1")
        expediente = self._crear_expediente(cfdi=self._crear_cfdi("CFDI-C"))
        datos = {
            "expediente": expediente,
            "empleado": empleado,
            "base_gravada": D("4000.00"),
            "monto_isn": D("100.00"),
            "area_codigo": "VENTAS",
            "sucursal": sucursal,
        }
        DistribucionISNEmpleado.objects.create(**datos)

        with self.assertRaises(IntegrityError), transaction.atomic():
            DistribucionISNEmpleado.objects.create(**datos)

    def test_periodo_debe_ser_el_primer_dia_del_mes(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-PERIODO"),
            periodo=date(2026, 8, 15),
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_revision_debe_ser_mayor_o_igual_a_uno(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-REVISION"),
            revision=0,
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_importe_pagado_debe_ser_positivo(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-IMPORTE"),
            importe_pagado=D("0.00"),
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_bases_del_expediente_no_admiten_negativos(self):
        casos = (
            ("base_gravada_calculada", "CFDI-BASE-CALCULADA"),
            ("base_declarada", "CFDI-BASE-DECLARADA"),
        )
        for revision, (campo, cfdi_uuid) in enumerate(casos, start=1):
            with self.subTest(campo=campo):
                datos = self._datos_expediente(
                    cfdi=self._crear_cfdi(cfdi_uuid),
                    uuid=cfdi_uuid,
                    revision=revision,
                    estado=ExpedienteISN.ESTADO_VALIDO,
                    aplicado_en=None,
                    **{campo: D("-0.01")},
                )
                with self.assertRaises(IntegrityError), transaction.atomic():
                    ExpedienteISN.objects.create(**datos)

    def test_estado_debe_ser_un_valor_permitido(self):
        datos = self._datos_expediente(
            cfdi=self._crear_cfdi("CFDI-ESTADO"),
            estado="INVENTADO",
            aplicado_en=None,
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(**datos)

    def test_estados_aplicados_exigen_fecha_de_aplicacion(self):
        for revision, estado in enumerate(
            (ExpedienteISN.ESTADO_APLICADO, ExpedienteISN.ESTADO_REEMPLAZADO),
            start=1,
        ):
            with self.subTest(estado=estado):
                datos = self._datos_expediente(
                    cfdi=self._crear_cfdi(f"CFDI-{estado}-SIN-FECHA"),
                    revision=revision,
                    estado=estado,
                    aplicado_en=None,
                )
                with self.assertRaises(IntegrityError), transaction.atomic():
                    ExpedienteISN.objects.create(**datos)

    def test_estados_sin_aplicacion_rechazan_fecha(self):
        for revision, estado in enumerate(
            (ExpedienteISN.ESTADO_VALIDO, ExpedienteISN.ESTADO_DISCREPANCIA),
            start=1,
        ):
            with self.subTest(estado=estado):
                datos = self._datos_expediente(
                    cfdi=self._crear_cfdi(f"CFDI-{estado}-CON-FECHA"),
                    revision=revision,
                    estado=estado,
                )
                with self.assertRaises(IntegrityError), transaction.atomic():
                    ExpedienteISN.objects.create(**datos)

    def test_reemplazado_conserva_fecha_de_aplicacion(self):
        expediente = self._crear_expediente(
            cfdi=self._crear_cfdi("CFDI-REEMPLAZADO-CON-FECHA")
        )
        aplicado_en = expediente.aplicado_en

        expediente.estado = ExpedienteISN.ESTADO_REEMPLAZADO
        try:
            with transaction.atomic():
                expediente.save(update_fields={"estado"})
        except IntegrityError:
            self.fail("REEMPLAZADO debe conservar la fecha de aplicación")

        expediente.refresh_from_db()
        self.assertEqual(expediente.estado, ExpedienteISN.ESTADO_REEMPLAZADO)
        self.assertEqual(expediente.aplicado_en, aplicado_en)

    def test_distribucion_no_admite_montos_negativos(self):
        sucursal = Sucursal.objects.create(codigo="S2", nombre="Sucursal 2")
        expediente = self._crear_expediente(cfdi=self._crear_cfdi("CFDI-DISTRIBUCION"))
        for numero, campo in enumerate(("base_gravada", "monto_isn"), start=1):
            with self.subTest(campo=campo):
                empleado = Empleado.objects.create(
                    codigo=f"E2-{numero}",
                    nombre=f"Empleado {numero}",
                )
                datos = {
                    "expediente": expediente,
                    "empleado": empleado,
                    "base_gravada": D("100.00"),
                    "monto_isn": D("10.00"),
                    "area_codigo": "VENTAS",
                    "sucursal": sucursal,
                }
                invalidos = {**datos, campo: D("-0.01")}
                with self.assertRaises(IntegrityError), transaction.atomic():
                    DistribucionISNEmpleado.objects.create(**invalidos)

    def test_uuid_se_sincroniza_desde_el_cfdi(self):
        cfdi = self._crear_cfdi("CFDI-UUID-CANONICO")

        expediente = self._crear_expediente(cfdi=cfdi, uuid="UUID-DIVERGENTE")

        expediente.refresh_from_db()
        self.assertEqual(expediente.uuid, cfdi.uuid)

    def test_update_fields_cfdi_explicito_persiste_cfdi_y_uuid_como_pareja(self):
        cfdi_a = self._crear_cfdi("CFDI-PAREJA-A")
        cfdi_b = self._crear_cfdi("CFDI-PAREJA-B")
        expediente = self._crear_expediente(cfdi=cfdi_a)

        expediente.cfdi = cfdi_b
        expediente.save(update_fields={"cfdi"})

        expediente.refresh_from_db()
        self.assertEqual(expediente.cfdi_id, cfdi_b.pk)
        self.assertEqual(expediente.uuid, expediente.cfdi.uuid)

    def test_update_fields_cfdi_id_explicito_persiste_pareja(self):
        cfdi_a = self._crear_cfdi("CFDI-ID-PAREJA-A")
        cfdi_b = self._crear_cfdi("CFDI-ID-PAREJA-B")
        expediente = self._crear_expediente(cfdi=cfdi_a)

        expediente.cfdi_id = cfdi_b.pk
        expediente.save(update_fields={"cfdi_id"})

        expediente.refresh_from_db()
        self.assertEqual(expediente.cfdi_id, cfdi_b.pk)
        self.assertEqual(expediente.uuid, expediente.cfdi.uuid)

    def test_update_fields_metadata_ignora_cfdi_y_uuid_incidentales(self):
        cfdi_a = self._crear_cfdi("CFDI-METADATA-A")
        cfdi_b = self._crear_cfdi("CFDI-METADATA-B")
        expediente = self._crear_expediente(cfdi=cfdi_a)

        expediente.cfdi = cfdi_b
        expediente.uuid = "UUID-INCIDENTAL"
        expediente.metadata = {"revision": "parcial"}
        expediente.save(update_fields={"metadata"})

        expediente.refresh_from_db()
        self.assertEqual(expediente.cfdi_id, cfdi_a.pk)
        self.assertEqual(expediente.uuid, cfdi_a.uuid)
        self.assertEqual(expediente.metadata, {"revision": "parcial"})

    def test_update_fields_uuid_aislado_se_rechaza_sin_divergir(self):
        cfdi = self._crear_cfdi("CFDI-UUID-AISLADO")
        expediente = self._crear_expediente(cfdi=cfdi)
        expediente.uuid = "UUID-AISLADO"

        with self.assertRaisesMessage(
            ValueError,
            "uuid solo puede actualizarse junto con cfdi",
        ):
            expediente.save(update_fields={"uuid"})

        expediente.refresh_from_db()
        self.assertEqual(expediente.uuid, expediente.cfdi.uuid)

    def test_update_fields_vacio_es_no_op(self):
        cfdi_a = self._crear_cfdi("CFDI-NO-OP-A")
        cfdi_b = self._crear_cfdi("CFDI-NO-OP-B")
        expediente = self._crear_expediente(cfdi=cfdi_a)
        expediente.cfdi = cfdi_b
        expediente.uuid = "UUID-NO-OP"
        expediente.metadata = {"no": "persistir"}

        with self.assertNumQueries(0):
            expediente.save(update_fields=[])

        expediente.refresh_from_db()
        self.assertEqual(expediente.cfdi_id, cfdi_a.pk)
        self.assertEqual(expediente.uuid, cfdi_a.uuid)
        self.assertEqual(expediente.metadata, {})

    def test_cfdi_de_un_expediente_esta_protegido(self):
        cfdi = self._crear_cfdi("CFDI-PROTEGIDO")
        self._crear_expediente(
            cfdi=cfdi,
            estado=ExpedienteISN.ESTADO_VALIDO,
            aplicado_en=None,
        )

        with self.assertRaises(ProtectedError):
            cfdi.delete()

"""Siembra las reglas de clasificacion bancaria con los patrones reales de las
cuentas de GRUPO EMPRESARIAL FONSMA (BanBajio Conecta, BBVA Maestra Pyme, AMEX
Business Gold), observados en estados de cuenta ene-jun 2026.

Idempotente: update_or_create por nombre. Correr tras cada ajuste de patrones.
"""
from __future__ import annotations

from django.core.management.base import BaseCommand

from conciliacion.models import ConceptoConciliacion, ReglaClasificacionMovimiento

# (nombre, concepto_codigo, tipo_movimiento, prioridad, patrones, confianza)
REGLAS = [
    # --- IVA antes que comision: "IVA COMISION..." tambien contiene "COMISION" ---
    ("IVA de comisiones bancarias", "IVA_COMISION_TPV", "cargo", 10,
     ["IVA COMISION", "IVA COM SERV", "IVA COMISION ADMINISTRACION"], 90),
    ("Comisiones bancarias y de terminal", "COMISION_TPV", "cargo", 20,
     ["COMISION APLICACION DE TASAS", "COMISION APLICACION TASAS", "COMISION POR DEPOSITO",
      "COMISION ADMINISTRACION DE PAQUETE", "SERV BANCA INTERNET"], 90),
    # --- Ingresos ---
    ("Deposito de ventas con tarjeta (adquirente)", "VENTA_TARJETA_DEBITO", "abono", 30,
     ["DEPOSITO NEGOCIOS AFILIADOS"], 85),
    ("Deposito de ventas en efectivo", "VENTA_EFECTIVO_SUCURSAL", "abono", 30,
     ["DEPOSITO EN EFECTIVO"], 90),
    # --- Traspasos entre cuentas propias ---
    ("Traspasos Conecta BanBajio", "TRASPASO_ENTRE_CUENTAS", "ambos", 40,
     ["TRASPASO DE RECURSOS A LA CUENTA CONECTA", "TRASPASO DE RECURSOS DE LA CUENTA CONECTA",
      "TRASPASO DE RECURSOS A LA CUENTA DE CHEQUES"], 90),
    ("SPEI entre cuentas propias BBVA<->BanBajio", "TRASPASO_ENTRE_CUENTAS", "ambos", 45,
     ["SPEI RECIBIDOBAJIO", "SPEI ENVIADO BAJIO"], 75),
    ("Pago recibido en tarjeta AMEX", "TRASPASO_ENTRE_CUENTAS", "abono", 45,
     ["GRACIAS POR SU PAGO"], 85),
    # --- Credito / tarjeta ---
    ("Disposicion de linea o plan de pagos", "DISPOSICION_LINEA_CREDITO", "abono", 50,
     ["DISPOSICION CREDITO", "PLAN PAGOS FIJOS"], 85),
    ("Pago a tarjeta de credito", "PAGO_TARJETA_CREDITO", "cargo", 50,
     ["PAGO TARJETA DE CREDITO"], 85),
    # --- Nomina y prestaciones ---
    ("Pago de nomina", "PAGO_NOMINA_TIMBRADA", "cargo", 60,
     ["PAGO DE NOMINA", "PAGO PRIMA VACACIONAL", "EDENRED", "INSTITUTO FONACOT"], 85),
    # --- Gastos / proveedores ---
    ("Pago de renta", "PAGO_RENTA_CFDI", "cargo", 70,
     ["RENTA DE ENERO", "RENTA DE FEBRERO", "RENTA DE MARZO", "RENTA DE ABRIL",
      "RENTA DE MAYO", "RENTA DE JUNIO", "RENTA DE JULIO", "RENTA DE AGOSTO",
      "RENTA DE SEPTIEMBRE", "RENTA DE OCTUBRE", "RENTA DE NOVIEMBRE", "RENTA DE DICIEMBRE",
      "SPEIRENTA"], 80),
    ("Pago a proveedor por SPEI con factura", "PAGO_PROVEEDOR_CFDI", "cargo", 75,
     ["SPEI:FACTURA", "SPEIFACTURA", "SPEI:PAGO FACTURA", "SPEIPAGO FACTURA",
      "SPEI:NOTA", "SPEINOTA", "PAGO DE SERVICIO", "RETIRO POR DOMICILIACION"], 80),
    ("Compra con tarjeta empresarial (POS)", "PAGO_PROVEEDOR_CFDI", "cargo", 80,
     ["COMPRA-DISPOSICION (POS)", "COMPRA-DISPOSICION"], 80),
    ("Gastos recurrentes tarjeta AMEX", "PAGO_PROVEEDOR_CFDI", "cargo", 85,
     ["AMAZON", "COSTCO", "MERCADO LIBRE", "TELEFONOS DE MEXICO", "PAYPAL",
      "OPENAI", "DIGITALOCEAN", "VOLARIS", "SAMS", "SAM S CLUB", "GASOL",
      "H&M", "PF CHANGS", "OPENPAY"], 75),
]

CONCEPTOS_NUEVOS = [
    {
        "codigo": "PAGO_PROVEEDOR_CFDI",
        "nombre": "Pago a proveedor con CFDI",
        "descripcion": "Pago a proveedor (SPEI, domiciliacion o tarjeta empresarial) que debe tener CFDI recibido.",
        "familia": ConceptoConciliacion.FAMILIA_GASTO,
        "tipo_movimiento": ConceptoConciliacion.TIPO_CARGO,
        "cfdi_esperado": ConceptoConciliacion.CFDI_RECIBIDO,
        "requiere_cfdi_recibido": True,
        "prioridad": 40,
    },
]


class Command(BaseCommand):
    help = "Siembra reglas de clasificacion bancaria (idempotente)"

    def handle(self, *args, **options):
        for data in CONCEPTOS_NUEVOS:
            codigo = data.pop("codigo")
            _, created = ConceptoConciliacion.objects.update_or_create(codigo=codigo, defaults=data)
            self.stdout.write(f"concepto {codigo}: {'creado' if created else 'actualizado'}")
            data["codigo"] = codigo

        creadas = actualizadas = 0
        for nombre, concepto_codigo, tipo, prioridad, patrones, confianza in REGLAS:
            concepto = ConceptoConciliacion.objects.get(codigo=concepto_codigo)
            _, created = ReglaClasificacionMovimiento.objects.update_or_create(
                nombre=nombre,
                defaults={
                    "concepto": concepto,
                    "tipo_movimiento": tipo,
                    "prioridad": prioridad,
                    "patrones_descripcion": patrones,
                    "confianza_base": confianza,
                    "activa": True,
                },
            )
            creadas += int(created)
            actualizadas += int(not created)
        self.stdout.write(self.style.SUCCESS(f"Reglas: {creadas} creadas, {actualizadas} actualizadas"))

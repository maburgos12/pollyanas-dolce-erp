from django.conf import settings
from django.db import models


class ConceptoConciliacion(models.Model):
    FAMILIA_VENTA = "venta"
    FAMILIA_TARJETA = "tarjeta"
    FAMILIA_TRANSFERENCIA = "transferencia"
    FAMILIA_GASTO = "gasto"
    FAMILIA_NOMINA = "nomina"
    FAMILIA_FISCAL = "fiscal"
    FAMILIA_BALANCE = "balance"
    FAMILIA_PENDIENTE = "pendiente"
    FAMILIA_CHOICES = [
        (FAMILIA_VENTA, "Venta"),
        (FAMILIA_TARJETA, "Tarjeta"),
        (FAMILIA_TRANSFERENCIA, "Transferencia"),
        (FAMILIA_GASTO, "Gasto"),
        (FAMILIA_NOMINA, "Nomina"),
        (FAMILIA_FISCAL, "Fiscal"),
        (FAMILIA_BALANCE, "Balance"),
        (FAMILIA_PENDIENTE, "Pendiente"),
    ]

    TIPO_ABONO = "abono"
    TIPO_CARGO = "cargo"
    TIPO_AMBOS = "ambos"
    TIPO_MOVIMIENTO_CHOICES = [
        (TIPO_ABONO, "Abono"),
        (TIPO_CARGO, "Cargo"),
        (TIPO_AMBOS, "Ambos"),
    ]

    CFDI_EMITIDO = "emitido"
    CFDI_RECIBIDO = "recibido"
    CFDI_NOMINA = "nomina"
    CFDI_PAGO = "pago"
    CFDI_EGRESO = "egreso"
    CFDI_NINGUNO = "ninguno"
    CFDI_OPCIONAL = "opcional"
    CFDI_ESPERADO_CHOICES = [
        (CFDI_EMITIDO, "CFDI emitido"),
        (CFDI_RECIBIDO, "CFDI recibido"),
        (CFDI_NOMINA, "Nomina timbrada"),
        (CFDI_PAGO, "Complemento de pago"),
        (CFDI_EGRESO, "CFDI de egreso"),
        (CFDI_NINGUNO, "Sin CFDI esperado"),
        (CFDI_OPCIONAL, "CFDI opcional"),
    ]

    codigo = models.CharField(max_length=80, unique=True)
    nombre = models.CharField(max_length=160)
    descripcion = models.TextField(blank=True)
    familia = models.CharField(max_length=30, choices=FAMILIA_CHOICES)
    tipo_movimiento = models.CharField(max_length=10, choices=TIPO_MOVIMIENTO_CHOICES)
    cfdi_esperado = models.CharField(max_length=20, choices=CFDI_ESPERADO_CHOICES)
    forma_pago_esperada = models.CharField(max_length=2, blank=True)
    requiere_rep = models.BooleanField(default=False)
    requiere_cfdi_recibido = models.BooleanField(default=False)
    requiere_evidencia_externa = models.BooleanField(default=False)
    afecta_iva = models.BooleanField(default=False)
    afecta_isr = models.BooleanField(default=False)
    afecta_flujo = models.BooleanField(default=True)
    permite_conciliacion_automatica = models.BooleanField(default=False)
    tolerancia_monto = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    tolerancia_porcentaje = models.DecimalField(max_digits=7, decimal_places=4, default=0)
    cuenta_contable_sugerida = models.CharField(max_length=120, blank=True)
    palabras_clave = models.JSONField(default=list, blank=True)
    evidencia_requerida = models.JSONField(default=list, blank=True)
    prioridad = models.PositiveIntegerField(default=100)
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["prioridad", "codigo"]
        verbose_name = "Concepto de conciliacion"
        verbose_name_plural = "Conceptos de conciliacion"
        indexes = [
            models.Index(fields=["familia", "activo"]),
            models.Index(fields=["tipo_movimiento", "activo"]),
            models.Index(fields=["cfdi_esperado"]),
        ]

    def __str__(self) -> str:
        return f"{self.codigo} | {self.nombre}"


class CuentaContableConciliacion(models.Model):
    TIPO_ACTIVO = "activo"
    TIPO_PASIVO = "pasivo"
    TIPO_CAPITAL = "capital"
    TIPO_INGRESO = "ingreso"
    TIPO_COSTO = "costo"
    TIPO_GASTO = "gasto"
    TIPO_ORDEN = "orden"
    TIPO_CHOICES = [
        (TIPO_ACTIVO, "Activo"),
        (TIPO_PASIVO, "Pasivo"),
        (TIPO_CAPITAL, "Capital"),
        (TIPO_INGRESO, "Ingreso"),
        (TIPO_COSTO, "Costo"),
        (TIPO_GASTO, "Gasto"),
        (TIPO_ORDEN, "Orden"),
    ]
    NATURALEZA_DEUDORA = "deudora"
    NATURALEZA_ACREEDORA = "acreedora"
    NATURALEZA_CHOICES = [
        (NATURALEZA_DEUDORA, "Deudora"),
        (NATURALEZA_ACREEDORA, "Acreedora"),
    ]

    codigo = models.CharField(max_length=60, unique=True)
    nombre = models.CharField(max_length=180)
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES)
    naturaleza = models.CharField(max_length=20, choices=NATURALEZA_CHOICES)
    agrupador_sat = models.CharField(max_length=20, blank=True)
    cuenta_contpaqi = models.CharField(max_length=60, blank=True)
    descripcion = models.TextField(blank=True)
    activa = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["codigo"]
        verbose_name = "Cuenta contable de conciliacion"
        verbose_name_plural = "Cuentas contables de conciliacion"
        indexes = [
            models.Index(fields=["tipo", "activa"]),
            models.Index(fields=["agrupador_sat"]),
        ]

    def __str__(self) -> str:
        return f"{self.codigo} | {self.nombre}"


class CuentaBancariaPropia(models.Model):
    cuenta_bancaria = models.OneToOneField(
        "syncfy_client.CuentaBancaria",
        on_delete=models.PROTECT,
        related_name="catalogo_conciliacion",
    )
    alias = models.CharField(max_length=120)
    empresa_rfc = models.CharField(max_length=13, blank=True)
    clabe = models.CharField(max_length=18, blank=True)
    ultimos_digitos = models.CharField(max_length=8, blank=True)
    cuenta_contable = models.ForeignKey(
        CuentaContableConciliacion,
        on_delete=models.PROTECT,
        related_name="cuentas_bancarias",
    )
    moneda = models.CharField(max_length=10, default="MXN")
    activa = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["alias"]
        verbose_name = "Cuenta bancaria propia"
        verbose_name_plural = "Cuentas bancarias propias"
        indexes = [
            models.Index(fields=["empresa_rfc", "activa"]),
            models.Index(fields=["ultimos_digitos"]),
            models.Index(fields=["clabe"]),
        ]

    def __str__(self) -> str:
        return self.alias


class ContraparteConciliacion(models.Model):
    TIPO_CLIENTE = "cliente"
    TIPO_PROVEEDOR = "proveedor"
    TIPO_BANCO = "banco"
    TIPO_SAT = "sat"
    TIPO_CUENTA_PROPIA = "cuenta_propia"
    TIPO_TARJETA = "tarjeta_credito"
    TIPO_LINEA_CREDITO = "linea_credito"
    TIPO_OTRO = "otro"
    TIPO_CHOICES = [
        (TIPO_CLIENTE, "Cliente"),
        (TIPO_PROVEEDOR, "Proveedor"),
        (TIPO_BANCO, "Banco"),
        (TIPO_SAT, "SAT"),
        (TIPO_CUENTA_PROPIA, "Cuenta propia"),
        (TIPO_TARJETA, "Tarjeta de credito"),
        (TIPO_LINEA_CREDITO, "Linea de credito"),
        (TIPO_OTRO, "Otro"),
    ]

    tipo = models.CharField(max_length=30, choices=TIPO_CHOICES)
    nombre = models.CharField(max_length=180)
    rfc = models.CharField(max_length=13, blank=True)
    cuenta_contable = models.ForeignKey(
        CuentaContableConciliacion,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="contrapartes",
    )
    palabras_clave = models.JSONField(default=list, blank=True)
    activa = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["tipo", "nombre"]
        verbose_name = "Contraparte de conciliacion"
        verbose_name_plural = "Contrapartes de conciliacion"
        indexes = [
            models.Index(fields=["tipo", "activa"]),
            models.Index(fields=["rfc"]),
        ]

    def __str__(self) -> str:
        return f"{self.get_tipo_display()} | {self.nombre}"


class InstrumentoFinancieroConciliacion(models.Model):
    TIPO_TARJETA_CREDITO = "tarjeta_credito"
    TIPO_LINEA_CREDITO = "linea_credito"
    TIPO_CHOICES = [
        (TIPO_TARJETA_CREDITO, "Tarjeta de credito"),
        (TIPO_LINEA_CREDITO, "Linea de credito"),
    ]

    tipo = models.CharField(max_length=30, choices=TIPO_CHOICES)
    nombre = models.CharField(max_length=160)
    institucion = models.CharField(max_length=120)
    numero_referencia = models.CharField(max_length=60, blank=True)
    contraparte = models.ForeignKey(
        ContraparteConciliacion,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="instrumentos",
    )
    cuenta_bancaria_pago = models.ForeignKey(
        CuentaBancariaPropia,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="instrumentos_pagados",
    )
    cuenta_contable_pasivo = models.ForeignKey(
        CuentaContableConciliacion,
        on_delete=models.PROTECT,
        related_name="instrumentos_pasivo",
    )
    cuenta_contable_intereses = models.ForeignKey(
        CuentaContableConciliacion,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="instrumentos_intereses",
    )
    patrones_descripcion = models.JSONField(default=list, blank=True)
    evidencia_requerida = models.JSONField(default=list, blank=True)
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["tipo", "nombre"]
        verbose_name = "Instrumento financiero de conciliacion"
        verbose_name_plural = "Instrumentos financieros de conciliacion"
        indexes = [
            models.Index(fields=["tipo", "activo"]),
            models.Index(fields=["institucion"]),
        ]

    def __str__(self) -> str:
        return f"{self.get_tipo_display()} | {self.nombre}"


class ReglaClasificacionMovimiento(models.Model):
    TIPO_CARGO = "cargo"
    TIPO_ABONO = "abono"
    TIPO_AMBOS = "ambos"
    TIPO_MOVIMIENTO_CHOICES = [
        (TIPO_CARGO, "Cargo"),
        (TIPO_ABONO, "Abono"),
        (TIPO_AMBOS, "Ambos"),
    ]

    nombre = models.CharField(max_length=180)
    concepto = models.ForeignKey(
        ConceptoConciliacion,
        on_delete=models.PROTECT,
        related_name="reglas_clasificacion",
    )
    tipo_movimiento = models.CharField(max_length=10, choices=TIPO_MOVIMIENTO_CHOICES, default=TIPO_AMBOS)
    prioridad = models.PositiveIntegerField(default=100)
    patrones_descripcion = models.JSONField(default=list, blank=True)
    contraparte_tipo = models.CharField(max_length=30, choices=ContraparteConciliacion.TIPO_CHOICES, blank=True)
    instrumento_tipo = models.CharField(max_length=30, choices=InstrumentoFinancieroConciliacion.TIPO_CHOICES, blank=True)
    requiere_cuenta_propia_destino = models.BooleanField(default=False)
    cuenta_debe_sugerida = models.ForeignKey(
        CuentaContableConciliacion,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="reglas_debe",
    )
    cuenta_haber_sugerida = models.ForeignKey(
        CuentaContableConciliacion,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="reglas_haber",
    )
    evidencia_requerida = models.JSONField(default=list, blank=True)
    confianza_base = models.PositiveSmallIntegerField(default=70)
    activa = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["prioridad", "nombre"]
        verbose_name = "Regla de clasificacion bancaria"
        verbose_name_plural = "Reglas de clasificacion bancaria"
        indexes = [
            models.Index(fields=["tipo_movimiento", "activa"]),
            models.Index(fields=["prioridad", "activa"]),
            models.Index(fields=["contraparte_tipo"]),
            models.Index(fields=["instrumento_tipo"]),
        ]

    def __str__(self) -> str:
        return f"{self.nombre} -> {self.concepto.codigo}"


class SucursalIdentificadorFiscal(models.Model):
    TIPO_TEXTO = "texto"
    TIPO_REGEX = "regex"
    TIPO_CHOICES = [
        (TIPO_TEXTO, "Texto"),
        (TIPO_REGEX, "Regex"),
    ]

    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.PROTECT,
        related_name="identificadores_fiscales",
    )
    patron = models.CharField(max_length=120)
    tipo = models.CharField(max_length=10, choices=TIPO_CHOICES, default=TIPO_TEXTO)
    descripcion = models.CharField(max_length=180, blank=True)
    prioridad = models.PositiveIntegerField(default=100)
    activo = models.BooleanField(default=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["prioridad", "sucursal__codigo", "patron"]
        verbose_name = "Identificador fiscal de sucursal"
        verbose_name_plural = "Identificadores fiscales de sucursal"
        constraints = [
            models.UniqueConstraint(fields=["sucursal", "patron", "tipo"], name="uniq_identificador_fiscal_sucursal")
        ]
        indexes = [
            models.Index(fields=["activo", "prioridad"]),
            models.Index(fields=["tipo", "activo"]),
        ]

    def __str__(self) -> str:
        return f"{self.sucursal.codigo} | {self.patron}"


class CfdiSucursalResolucion(models.Model):
    FUENTE_XML_CONCEPTO = "xml_concepto"
    FUENTE_MANUAL = "manual"
    FUENTE_SIN_COINCIDENCIA = "sin_coincidencia"
    FUENTE_AMBIGUA = "ambigua"
    FUENTE_CHOICES = [
        (FUENTE_XML_CONCEPTO, "Concepto XML"),
        (FUENTE_MANUAL, "Manual"),
        (FUENTE_SIN_COINCIDENCIA, "Sin coincidencia"),
        (FUENTE_AMBIGUA, "Ambigua"),
    ]

    cfdi = models.OneToOneField(
        "sat_client.CfdiDescargado",
        on_delete=models.CASCADE,
        related_name="resolucion_sucursal",
    )
    sucursal = models.ForeignKey(
        "core.Sucursal",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="cfdi_resueltos",
    )
    fuente = models.CharField(max_length=30, choices=FUENTE_CHOICES)
    confianza = models.PositiveSmallIntegerField(default=0)
    texto_detectado = models.CharField(max_length=255, blank=True)
    detalles = models.JSONField(default=dict, blank=True)
    revisado = models.BooleanField(default=False)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-actualizado_en"]
        verbose_name = "Resolucion de sucursal CFDI"
        verbose_name_plural = "Resoluciones de sucursal CFDI"
        indexes = [
            models.Index(fields=["sucursal", "confianza"]),
            models.Index(fields=["fuente"]),
            models.Index(fields=["revisado"]),
        ]

    def __str__(self) -> str:
        sucursal = self.sucursal.codigo if self.sucursal_id else "sin sucursal"
        return f"{self.cfdi.uuid} | {sucursal} | {self.confianza}"


class ImportacionBancaria(models.Model):
    FUENTE_MANUAL_CSV = "manual_csv"
    FUENTE_MANUAL_EXCEL = "manual_excel"
    FUENTE_MANUAL_PDF = "manual_pdf"
    FUENTE_CHOICES = [
        (FUENTE_MANUAL_CSV, "Carga manual CSV"),
        (FUENTE_MANUAL_EXCEL, "Carga manual Excel"),
        (FUENTE_MANUAL_PDF, "Carga manual PDF"),
    ]

    ESTADO_PREVIEW = "preview"
    ESTADO_IMPORTADA = "importada"
    ESTADO_ERROR = "error"
    ESTADO_CHOICES = [
        (ESTADO_PREVIEW, "Preview"),
        (ESTADO_IMPORTADA, "Importada"),
        (ESTADO_ERROR, "Error"),
    ]

    cuenta = models.ForeignKey(
        "syncfy_client.CuentaBancaria",
        on_delete=models.PROTECT,
        related_name="importaciones_bancarias",
    )
    fuente = models.CharField(max_length=20, choices=FUENTE_CHOICES)
    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default=ESTADO_PREVIEW)
    archivo_nombre = models.CharField(max_length=255)
    archivo_hash = models.CharField(max_length=64, db_index=True)
    total_filas = models.IntegerField(default=0)
    movimientos_nuevos = models.IntegerField(default=0)
    movimientos_duplicados = models.IntegerField(default=0)
    filas_con_error = models.IntegerField(default=0)
    preview = models.JSONField(default=list, blank=True)
    errores = models.JSONField(default=list, blank=True)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Importacion bancaria"
        verbose_name_plural = "Importaciones bancarias"
        indexes = [
            models.Index(fields=["cuenta", "creado_en"]),
            models.Index(fields=["estado"]),
        ]

    def __str__(self) -> str:
        return f"{self.cuenta} | {self.archivo_nombre} | {self.estado}"

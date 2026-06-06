from __future__ import annotations

from dataclasses import dataclass

from .models import Empleado


@dataclass(frozen=True)
class FuncionOperativa:
    codigo: str
    etiqueta: str
    departamento_origen: str
    departamento_actual: str
    puesto_operativo: str
    nivel_organizacional: str = Empleado.NIVEL_COLABORADOR


FUNCIONES_OPERATIVAS: tuple[FuncionOperativa, ...] = (
    FuncionOperativa("HORNOS", "Hornos", Empleado.DEP_PRODUCCION, Empleado.DEP_PRODUCCION, "HORNOS"),
    FuncionOperativa("EMBETUNADO", "Embetunado", Empleado.DEP_PRODUCCION, Empleado.DEP_PRODUCCION, "EMBETUNADO"),
    FuncionOperativa("ARMADO", "Armado", Empleado.DEP_PRODUCCION, Empleado.DEP_PRODUCCION, "ARMADO"),
    FuncionOperativa("CRUCERO", "Crucero", Empleado.DEP_PRODUCCION, Empleado.DEP_PRODUCCION, "CRUCERO"),
    FuncionOperativa(
        "ENVIO A SUCURSAL",
        "Envío a sucursal",
        Empleado.DEP_LOGISTICA,
        Empleado.DEP_PRODUCCION,
        "ENVIO_SUCURSAL",
    ),
    FuncionOperativa("CAJAS", "Cajas", Empleado.DEP_VENTAS, Empleado.DEP_VENTAS, "CAJAS"),
    FuncionOperativa("AUXILIAR CAJAS", "Auxiliar cajas", Empleado.DEP_VENTAS, Empleado.DEP_VENTAS, "AUXILIAR_CAJAS"),
    FuncionOperativa("CALL CENTER", "Call center", Empleado.DEP_VENTAS, Empleado.DEP_VENTAS, "CALL_CENTER"),
    FuncionOperativa("REPARTIDORES", "Repartidores", Empleado.DEP_LOGISTICA, Empleado.DEP_VENTAS, "REPARTIDOR"),
    FuncionOperativa("COMPRAS", "Compras", Empleado.DEP_COMPRAS, Empleado.DEP_COMPRAS, "COMPRAS"),
    FuncionOperativa("ALMACEN", "Almacén", Empleado.DEP_ADMINISTRACION, Empleado.DEP_ADMINISTRACION, "ALMACEN"),
    FuncionOperativa("LIMPIEZA", "Limpieza / afanadoras", Empleado.DEP_ADMINISTRACION, Empleado.DEP_ADMINISTRACION, "LIMPIEZA"),
    FuncionOperativa(
        "AUXILIAR CONTABLE",
        "Auxiliar contable",
        Empleado.DEP_ADMINISTRACION,
        Empleado.DEP_ADMINISTRACION,
        "AUXILIAR_CONTABLE",
    ),
    FuncionOperativa("RRHH", "Recursos Humanos", Empleado.DEP_RRHH, Empleado.DEP_RRHH, "RRHH"),
    FuncionOperativa(
        "MANTENIMIENTO",
        "Mantenimiento",
        Empleado.DEP_MANTENIMIENTO,
        Empleado.DEP_MANTENIMIENTO,
        "MANTENIMIENTO",
    ),
    FuncionOperativa("MARKETING", "Marketing externo", Empleado.DEP_MARKETING, Empleado.DEP_MARKETING, "MARKETING"),
)

AREA_DIVISION_CHOICES = tuple(
    (
        funcion.codigo,
        funcion.etiqueta,
        funcion.departamento_origen,
        funcion.departamento_actual,
        funcion.puesto_operativo,
    )
    for funcion in FUNCIONES_OPERATIVAS
)
AREA_DIVISION_VALUES = frozenset(funcion.codigo for funcion in FUNCIONES_OPERATIVAS)
AREA_DIVISION_MAP = {
    funcion.codigo: {
        "departamento_origen": funcion.departamento_origen,
        "departamento": funcion.departamento_actual,
        "puesto_operativo": funcion.puesto_operativo,
        "nivel_organizacional": funcion.nivel_organizacional,
    }
    for funcion in FUNCIONES_OPERATIVAS
}

PUESTO_OPERATIVO_CHOICES = tuple(
    (funcion.puesto_operativo, funcion.etiqueta)
    for funcion in FUNCIONES_OPERATIVAS
    if funcion.puesto_operativo
)
PUESTO_OPERATIVO_VALUES = frozenset(value for value, _label in PUESTO_OPERATIVO_CHOICES)

NIVEL_ORGANIZACIONAL_CHOICES = Empleado.NIVEL_ORGANIZACIONAL_CHOICES
NIVEL_ORGANIZACIONAL_VALUES = frozenset(value for value, _label in NIVEL_ORGANIZACIONAL_CHOICES)

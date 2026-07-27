# Contrato de detalle de nota Point

Fecha de verificación: 2026-07-27. Entorno: sesión autenticada de Point en producción,
exclusivamente mediante solicitudes `GET`. La nota usada fue reciente; sus
identificadores y valores se mantuvieron fuera del repositorio y de esta evidencia.

## Decisión

`DIRECT_API`

Point expone líneas por `PK_NOTA` mediante endpoints usados por su propia interfaz.
No es necesario ni válido reconstruir productos desde ventas o totales agregados.

`POINT_CUSTOMERS_AVAILABLE`

Point expone un identificador estable de cliente y campos de nombre, teléfonos y
correo. La importación debe enlazar por el identificador estable y teléfono; los
campos pueden venir vacíos. No se debe inferir cliente desde cajero, agregados ni el
texto visible de ventas.

## Evidencia de red

La pantalla oficial `GET /Report/NotasIdx` carga
`GET /Scripts/ReportesNotas.js`. Al seleccionar una nota, ese código ejecuta:

| Uso | Método | Path | Parámetros |
| --- | --- | --- | --- |
| Cabecera | `GET` | `/Clientes/get_encabezado_nota/` | query `id_nota=<PK_NOTA>` |
| Líneas | `GET` | `/Clientes/get_detalle_nota/` | query `id_nota=<PK_NOTA>` |

La cabecera respondió `application/json` con una lista de un elemento y estos
campos: `Cajero`, `Cliente`, `FK_Cajero`, `FK_Cliente`, `FK_Sucursal`,
`Fecha_Hora`, `Folio`, `Importe`, `MotivoCancelacion`, `Sucursal`, `isCredito`,
`isFacturado`.

El detalle respondió HTTP 200 como `text/plain`, pero su cuerpo es un arreglo JSON.
Cada línea incluyó: `Codigo`, `Producto`, `Cantidad`, `Precio_Venta`, `Descuento`,
`Descuento_p`, `Total`, `autoriza`, `jsonDescuentos`.

## Semántica monetaria verificada

El 2026-07-27 se hizo una segunda verificación exclusivamente mediante `GET`, sin
guardar ni imprimir identificadores, productos, clientes ni otros datos personales.
Se localizaron dos notas recientes representativas:

- una línea con `Cantidad=2`, `Precio_Venta=70`, `Descuento=0` y `Total=140`;
  la cabecera tuvo `Importe=140` y la suma de `Total` de sus tres líneas fue `140`;
- una línea con `Cantidad=1`, `Precio_Venta=395`, `Descuento=19.987`,
  `Descuento_p=0.0506` y `Total=375.013`; la cabecera tuvo `Importe=375.01`
  y la suma cruda de `Total` de sus dos líneas fue `375.013`.

Esto confirma que Point puede entregar descuentos y totales con subcentavos. En
los ejemplos observados la aritmética de cada línea fue consistente, pero dos
notas no bastan para declarar una fórmula universal de descuentos o impuestos.
La regla segura implementada es conservar `Total` como hecho autoritativo de
Point, sumar sus valores crudos y redondear una sola vez a centavos antes de
comparar con `Importe`. Los importes del objeto canónico se exponen a dos
decimales. No se compara `Importe` contra la suma de líneas ya redondeadas ni se
reconstruye `Total` desde cantidad, precio o descuento.

Comportamiento de error observado en el endpoint de líneas:

- sin `id_nota`: HTTP 500 con HTML genérico;
- `id_nota` inexistente: HTTP 200 y cuerpo `[]`;
- `id_nota` existente: HTTP 200 y arreglo JSON de líneas.

## Cliente

La cabecera de nota incluyó `FK_Cliente`. En la misma sesión, la pantalla oficial
`GET /Clientes/Index` carga `GET /Clientes/tab_clientes`; su búsqueda usa:

`GET /Clientes/get_clientes_byActivo`

Parámetros observados: `activo`, `id_sucursal`, `texto`,
`id_tipo_membresia`. La respuesta es JSON servido como `text/plain` e incluye
`Pk_cliente`, `Nombre_completo`, `telefono1`, `telefono2` y `email`, además de
otros campos del catálogo.

En la nota revisada, `FK_Cliente` coincidió exactamente con `Pk_cliente` en la
respuesta del catálogo. Existían nombre y al menos un teléfono; el correo estaba
vacío. Esto confirma disponibilidad del contrato, no completitud de todos los
campos para todos los clientes.

## Seguridad del probe

`probe_point_note_detail` exige `--pk-nota`, acepta `--folio`, `--sucursal` y
`--output`, reutiliza `PointHttpSessionService` y cierra la sesión incluso si falla
la autenticación o la selección del workspace.

La salida usa una lista permitida, no una lista de secretos conocidos. Solo
conserva identificadores y campos operativos explícitos de cabecera y líneas;
cualquier campo desconocido o de texto libre se redacta como `***`. Esto incluye
`Cliente`, `Cajero`, `FK_Cliente`, `FK_Cajero`, `MotivoCancelacion`, `autoriza`,
`jsonDescuentos`, cookies, tokens y claves PII inesperadas.

El probe y el normalizador comparten el mismo esquema mínimo. El probe solo declara
`DIRECT_API` después de comprobar que cabecera y detalle son listas no vacías de
objetos con todos los campos que el normalizador requiere. HTML de sesión
expirada, `null`, objetos, strings, JSON inválido, líneas vacías y errores HTTP
producen `CommandError` con el endpoint de cabecera o detalle identificado.

El timeout HTTP respeta `POINT_TIMEOUT` y la escritura es atómica: crea un temporal
en el mismo directorio, sincroniza y usa `os.replace`. Un fallo conserva el archivo
anterior y elimina el temporal. El comando no imprime la respuesta.

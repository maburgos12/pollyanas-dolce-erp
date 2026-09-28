# Auditoría mensual de trazabilidad de producto e inventario

## Estado

Diseño aprobado conceptualmente por Dirección el 27 de septiembre de 2026. Este documento define el comportamiento esperado; no autoriza por sí solo cambios en datos operativos ni sustituye el plan de implementación.

## Problema

El reporte actual compara cantidades mensuales, pero una diferencia no permite reconstruir de forma sencilla qué ocurrió con el producto. Mezcla en una misma lectura el cierre de Point, el cálculo teórico y la posible causa, aunque todavía no exista evidencia para atribuir la diferencia a una merma, conversión, transferencia u otro movimiento.

La auditoría debe responder, por producto y ubicación:

1. Con cuánto inició el mes.
2. Qué se produjo o recibió.
3. Qué se vendió, mermó, convirtió o transfirió.
4. Cuánto debió quedar.
5. Cuánto reportó Point al cierre.
6. Más adelante, cuánto contó físicamente la sucursal.
7. Qué movimiento explica cada diferencia y quién aprobó la aclaración.

## Objetivos

- Conciliar mensualmente cada producto por sucursal, CEDIS y almacén Devoluciones.
- Relacionar productos enteros con sus presentaciones derivadas, como rebanadas o vasos.
- Relacionar las dos partes de transferencias y conversiones sin duplicar entradas o salidas.
- Distinguir un cierre Point protegido de una conciliación de movimientos completa.
- Incorporar después los cierres manuales de sucursales sin reemplazar ni alterar la evidencia de Point.
- Mantener una operación sencilla: mostrar primero excepciones y dejar el detalle completo bajo demanda.
- Conservar trazabilidad de fuentes, usuarios, aclaraciones, evidencias y aprobaciones.

## Fuera de alcance

- Modificar movimientos históricos de Point para forzar un cuadre.
- Inferir automáticamente que toda diferencia es merma.
- Crear un catálogo alterno de productos o ubicaciones ajeno a Point.
- Sustituir la captura operativa de producción, ventas, mermas, conversiones, transferencias o conteos.
- Exigir conteos físicos manuales para poder proteger el cierre Point de un mes.

## Enfoques considerados

### 1. Tabla mensual ampliada

Agregar más columnas al reporte actual sería rápido, pero mantendría mezcladas las causas, aumentaría el desplazamiento y no permitiría seguir una cadena de movimientos con claridad.

### 2. Libro completo de movimientos

Mostrar todos los movimientos ofrece detalle técnico, pero obliga al usuario a revisar cientos o miles de registros y no prioriza los casos que requieren una decisión.

### 3. Auditoría por excepciones con trazabilidad completa

Es el enfoque elegido. La pantalla principal resume el estado y presenta primero los casos pendientes. Cada caso permite abrir la cadena completa de movimientos y su evidencia original.

## Unidades de auditoría

La unidad mínima es la combinación de:

- mes operativo;
- ubicación Point;
- producto o presentación Point.

Las ubicaciones auditables son:

- cada sucursal;
- CEDIS;
- almacén Devoluciones.

Devoluciones se trata como una ubicación con inventario propio. Su existencia pertenece a la empresa, pero no se considera disponible para venta mientras permanezca ahí. Una transferencia hacia Devoluciones cambia la custodia del producto; no constituye merma ni salida definitiva.

El total empresa será una suma de ubicaciones y servirá como resumen. Nunca deberá compensar silenciosamente el faltante de una ubicación con el sobrante de otra.

## Modelo de conciliación mensual

Para cada producto y ubicación, el saldo esperado se calcula así:

```text
inventario inicial Point
+ producción
+ transferencias recibidas
+ conversiones de entrada
- ventas
- mermas
- transferencias enviadas
- conversiones de salida
+/- ajustes de inventario identificados
= inventario esperado
```

El inventario esperado se compara contra el cierre Point de esa ubicación:

```text
diferencia de movimientos = cierre Point - inventario esperado
```

El cierre del último día del mes anterior es la apertura del mes auditado. El cierre del último día del mes auditado es la base de apertura del mes siguiente. La hora de recepción del reporte no cambia la fecha operativa del cierre.

## Dos conciliaciones independientes

### Conciliación de movimientos

Comprueba si el inventario inicial y los movimientos identificados explican el cierre Point. Puede realizarse aunque todavía no exista un conteo físico manual.

### Conciliación física

Cuando exista cierre manual de una sucursal, compara:

```text
diferencia física = conteo manual - cierre Point
```

Esta separación permite distinguir:

- movimientos que no explican el saldo de Point;
- Point conciliado, pero existencia física distinta;
- movimientos, Point y conteo físico completamente conciliados;
- ausencia de conteo manual, que no debe presentarse como error de fuente.

El cierre Point puede permanecer protegido y servir como apertura del mes siguiente aunque la conciliación de movimientos o la conciliación física todavía tengan excepciones abiertas.

## Trazabilidad de conversiones

Los productos derivados se agrupan en familias de presentaciones mediante las relaciones canónicas existentes en el ERP y Point. Una conversión completa relaciona:

- producto y cantidad de origen;
- producto y cantidad resultante;
- equivalencia esperada;
- ubicación donde ocurrió;
- fecha, usuario y referencia de Point;
- merma asociada, cuando exista como movimiento independiente.

Ejemplo: si una sucursal convierte un pastel entero en doce rebanadas, la auditoría espera una salida de un pastel y una entrada de doce rebanadas en la misma ubicación. Las ventas y mermas posteriores se aplican a las rebanadas, no nuevamente al pastel de origen.

Una conversión puede quedar en uno de estos estados:

- conciliada;
- falta producto de origen;
- falta producto resultante;
- equivalencia fuera de la configuración vigente;
- merma de conversión pendiente de relacionar;
- cantidad sin trazabilidad suficiente.

La auditoría puede proponer relaciones candidatas, pero no debe crear ni dar por cierta una conversión sin evidencia.

## Trazabilidad de transferencias

Una transferencia completa relaciona una salida del origen y una entrada en el destino. La relación usa la evidencia disponible de Point: referencia, producto, cantidad, fecha, origen y destino.

Reglas:

- una transferencia no cambia el inventario total de la empresa;
- cada lado modifica únicamente la ubicación correspondiente;
- una entrada o salida sin contraparte genera una excepción de transferencia incompleta;
- enviar producto a Devoluciones no es merma;
- desde Devoluciones el producto puede regresar a una sucursal, convertirse, mermarse o permanecer en inventario;
- la disposición posterior del producto no puede duplicar el efecto de la transferencia original.

## Mermas, producción, ventas y ajustes

- La producción aumenta el producto en la ubicación donde se registra.
- La venta disminuye el producto vendible en la sucursal correspondiente.
- La merma disminuye el producto en la ubicación donde ocurrió.
- Un ajuste de inventario participa en la fórmula solo si Point lo identifica como movimiento y conserva su referencia.
- El puesto o área del usuario que capturó el movimiento no determina su ubicación contable. La ubicación y la custodia del inventario sí la determinan.
- Una persona de Ventas puede registrar una conversión o merma válida, y una persona de Producción puede hacerlo en CEDIS. La auditoría conserva al actor, pero asigna el caso según la ubicación afectada.

## Casos de auditoría y aprobación

Cuando el cierre Point no coincide con el inventario esperado, el ERP crea o actualiza un caso de auditoría sin modificar la fuente original.

Un caso contiene:

- mes, producto y ubicación;
- diferencia detectada;
- cadena de movimientos encontrada;
- relaciones candidatas y evidencia utilizada;
- causa declarada, comentario y evidencia adjunta cuando corresponda;
- persona que explicó el caso;
- persona que lo aprobó;
- fechas y bitácora de cambios.

Una diferencia solo se considera resuelta cuando:

1. movimientos verificables explican completamente la cantidad; o
2. una responsable registra una aclaración y otra responsable autorizada la aprueba.

Quien registra una aclaración no puede aprobarla por sí mismo. La aprobación se asigna por custodia:

- casos de CEDIS: responsable autorizada de Producción/CEDIS;
- casos de sucursal: responsable autorizada de la sucursal;
- excepciones relevantes o fuera de tolerancia: Dirección o Administración.

Los umbrales monetarios o cuantitativos no se inventarán durante la implementación; deberán configurarse y aprobarse antes de activar una regla de escalamiento automático.

## Estados

### Estado del cierre Point

- disponible;
- protegido.

### Estado de conciliación de movimientos

- conciliado;
- pendiente de explicación;
- explicación pendiente de aprobación;
- resuelto y aprobado;
- fuente incompleta.

### Estado de conciliación física

- sin conteo manual;
- conteo recibido;
- diferencia física pendiente;
- diferencia física aprobada;
- conciliado físicamente.

Los tres estados se muestran separados. `Protegido` nunca debe presentarse como sinónimo de `conciliado`.

## Experiencia de usuario

La vista mensual inicia con tres indicadores comprensibles:

- productos que cuadran con Point;
- casos pendientes de explicar o aprobar;
- ubicaciones pendientes de conteo físico.

La lista principal muestra primero las excepciones. Cada fila contiene únicamente:

- producto;
- ubicación;
- cantidad de diferencia;
- causa candidata, si existe;
- estado;
- acción para revisar.

Los productos conciliados permanecen en una pestaña secundaria. El encabezado de las tablas se conserva visible y alineado durante el desplazamiento.

Al abrir un caso se presenta una secuencia legible:

1. inventario inicial;
2. producción y entradas;
3. ventas y salidas;
4. conversiones;
5. transferencias;
6. mermas y ajustes;
7. inventario esperado;
8. cierre Point;
9. conteo físico, cuando exista.

Cada paso enlaza con su evidencia original. La interfaz usa términos operativos y evita exponer nombres de tablas, jobs o estructuras internas.

## Arquitectura funcional

La solución se divide en unidades independientes:

1. **Adaptadores de fuentes:** leen cierres Point, producción, ventas, mermas, conversiones, transferencias, ajustes y conteos manuales sin reescribirlos.
2. **Normalizador de movimientos:** expresa cada movimiento con mes, fecha, ubicación, producto, cantidad, dirección, actor y referencia de origen.
3. **Relacionador de trazabilidad:** empareja transferencias, conversiones y presentaciones derivadas; conserva candidatos ambiguos sin decidir por intuición.
4. **Motor de balance mensual:** calcula saldos esperados por producto y ubicación.
5. **Gestor de casos:** conserva explicaciones, evidencias, aprobaciones y estados.
6. **Proyección de lectura:** entrega resumen, excepciones y detalle bajo demanda para que la pantalla no tenga que recalcular o representar todo el historial en cada carga.
7. **Adaptador de conteos físicos:** en una etapa posterior, consume el módulo canónico de conteos de sucursal; no crea una captura paralela.

## Integridad y manejo de errores

- Una fuente ausente o incompleta produce `fuente incompleta`, no un cero.
- Un movimiento ambiguo permanece sin relacionar; no se asigna a la coincidencia más cercana sin evidencia suficiente.
- Las cantidades negativas, equivalencias imposibles y ubicaciones desconocidas generan excepciones explícitas.
- Una recarga de Point puede actualizar la proyección y los candidatos, pero no borra aprobaciones ni evidencia sin dejar historial.
- Los cambios posteriores en catálogos o equivalencias no deben reescribir silenciosamente una auditoría histórica protegida.
- Ninguna aclaración manual modifica los movimientos originales de Point.
- Las operaciones deben respetar la zona horaria `America/Mazatlan` y las fechas operativas del movimiento.

## Rendimiento

- El usuario consulta un solo mes a la vez.
- La pantalla carga primero el resumen y las excepciones; el detalle se obtiene al abrir un caso.
- El cálculo mensual se materializa después de sincronizar las fuentes o al reconstruir explícitamente el periodo.
- No se recorrerán todos los meses ni todos los snapshots históricos para abrir la pantalla.
- La recarga debe ser idempotente: los mismos movimientos producen el mismo balance y no duplican casos.

## Etapas de entrega

### Etapa 1: conciliación de movimientos Point

- balance por producto y ubicación;
- CEDIS y Devoluciones como ubicaciones auditables;
- relaciones de transferencias y conversiones;
- casos, explicaciones y aprobación;
- vista por excepciones y detalle de trazabilidad.

### Etapa 2: conciliación física

- integración con cierres manuales de sucursales;
- comparación del conteo contra el cierre Point protegido;
- casos de diferencia física y su aprobación;
- cobertura de conteos por ubicación.

La Etapa 1 no debe bloquearse por la ausencia actual de conteos manuales. La arquitectura deberá dejar definido el contrato de integración para incorporarlos sin rehacer el balance mensual.

## Criterios de aceptación

- El reporte calcula por separado cada producto en cada ubicación.
- El total empresa no oculta diferencias compensadas entre ubicaciones.
- CEDIS y Devoluciones aparecen como ubicaciones auditables con inventario propio.
- Una transferencia completa conserva el total empresa y cambia únicamente origen y destino.
- Una conversión completa consume el origen y genera la presentación resultante según su equivalencia.
- Una transferencia o conversión incompleta produce una excepción trazable.
- El cierre Point protegido y la conciliación se presentan como estados distintos.
- Una ausencia de fuente nunca se representa como cantidad cero.
- Una aclaración requiere un registrante y un aprobador distintos.
- El usuario puede reconstruir el origen y destino de cualquier cantidad desde el detalle del caso.
- La pantalla principal prioriza excepciones y mantiene el detalle completo bajo demanda.
- Cuando existan conteos manuales, la diferencia física se calcula contra el cierre Point, no contra el saldo teórico.

## Estrategia de pruebas

### Pruebas unitarias

- fórmula de balance para cada tipo de movimiento;
- emparejamiento completo e incompleto de transferencias;
- conversiones con equivalencia correcta e incorrecta;
- permanencia, merma y retransferencia desde Devoluciones;
- separación entre diferencia de movimientos y diferencia física;
- reglas de aprobación y prohibición de autoaprobación;
- idempotencia de reconstrucciones.

### Pruebas de integración

- mes completo con varias sucursales, CEDIS y Devoluciones;
- familia de producto entero y rebanadas;
- fuentes faltantes o fallidas;
- cambios de catálogo posteriores al cierre;
- integración futura con conteos manuales canónicos.

### Validación operativa

- reconstruir agosto de 2026 sin alterar su cierre Point protegido;
- revisar una muestra de productos conciliados y casos pendientes por ubicación;
- comprobar una cadena real de producción, transferencia, conversión, venta y merma;
- validar con responsables de sucursal y CEDIS que la lectura coincide con su operación;
- confirmar en navegador que el resumen es comprensible y que la trazabilidad detallada puede auditarse sin consultar directamente la base de datos.

## Resultado esperado

El ERP dejará de limitarse a mostrar diferencias. Permitirá encontrar la cadena de custodia y transformación de cada producto, distinguir qué está demostrado de qué sigue sin explicación y sostener tanto el cierre mensual de Point como los futuros conteos físicos de sucursales.

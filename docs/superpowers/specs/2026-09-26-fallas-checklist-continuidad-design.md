# Diseño: continuidad de fallas detectadas en checklists diarios

Fecha: 2026-09-26

Estado: aprobado para convertir en plan de implementación

Alcance: checklists diarios de Higiene y limpieza, reportes de falla y notificaciones de Mantenimiento

## Problema

Cada captura diaria de Higiene y limpieza conserva su propia revisión. Sin embargo, cuando un mismo punto continúa en estado `No cumple` al día siguiente, el flujo actual crea otro `ReporteFalla` y nuevas notificaciones. Esto multiplica tarjetas para Mantenimiento y oculta que se trata de una sola incidencia que lleva varios días sin resolverse.

La captura diaria y el reporte de falla tienen propósitos distintos:

- el checklist demuestra qué se revisó cada día, quién lo revisó y qué encontró;
- el reporte de falla representa el problema que Mantenimiento debe atender hasta su cierre.

El diseño debe conservar ambas evidencias sin convertir cada constatación diaria en una incidencia nueva.

## Objetivos

1. Permitir que varias respuestas de checklists diarios documenten la continuidad de una misma falla principal.
2. Evitar nuevos reportes y notificaciones cuando la sucursal confirma que el problema sigue igual.
3. Permitir que la persona indique que se trata de otro problema aunque ocurra en el mismo punto.
4. Mostrar a Mantenimiento la primera detección, la última confirmación, los días transcurridos y toda la evidencia diaria.
5. Consolidar repeticiones históricas mediante una vista previa auditable, sin eliminar registros, folios ni evidencias.

## Fuera de alcance

- Cerrar automáticamente una falla porque la sucursal marque el punto como corregido.
- Eliminar reportes, notificaciones, fotos o bitácoras históricas.
- Deduplicar reportes manuales que no provengan de los checklists diarios.
- Cambiar las reglas de permisos de Higiene, Fallas o Mantenimiento.
- Modificar datos históricos sin una vista previa revisada y una ejecución explícita.

## Conceptos

### Falla principal

Es el `ReporteFalla` que Mantenimiento atiende. Conserva su folio, estado, prioridad, responsable y ciclo de resolución.

### Constatación diaria

Es una `RespuestaHigiene` capturada en una fecha concreta que confirma que la falla sigue igual, cambió, empeoró o parece corregida. Cada constatación conserva su registro, autora, observación y evidencia.

### Reincidencia

Es una nueva aparición después del cierre de la falla anterior. Genera otro reporte y no se mezcla con el ciclo ya cerrado.

### Reporte repetido histórico

Es un reporte ya creado que corresponde al mismo problema y al mismo periodo abierto que otro reporte principal. Se conserva y se vincula mediante `duplicado_de`; no se borra ni se reescribe como si nunca hubiera existido.

## Identidad de coincidencia

La búsqueda de una falla activa se realizará con información estructurada, no comparando únicamente títulos o texto libre:

- sucursal;
- tipo de checklist;
- `punto_clave` exacto;
- tipo de objetivo: equipo o instalación;
- equipo concreto, cuando aplique;
- área de instalación normalizada, cuando aplique;
- categoría de falla.

Una coincidencia propone continuidad, pero no decide silenciosamente por la persona. El mismo punto puede presentar problemas distintos.

## Flujo diario en la sucursal

1. La persona llena el checklist como lo hace actualmente.
2. Al marcar `No cumple` y solicitar seguimiento, el sistema consulta si existe una falla principal activa con identidad coincidente.
3. Si no existe, se conserva el flujo actual de clasificación, evidencia y creación de reporte.
4. Si existe, la interfaz muestra el folio, título, estado, fecha inicial y última confirmación.
5. La persona elige una de estas acciones:
   - **Sigue igual:** enlaza la respuesta de hoy a la falla existente. La foto es opcional.
   - **Empeoró o cambió:** enlaza la respuesta, exige comentario y evidencia, y genera un aviso de cambio relevante.
   - **Es otro problema:** conserva la clasificación completa y crea una falla independiente.
   - **Ya quedó corregido:** registra la constatación y solicita validación a Mantenimiento; no cierra automáticamente.
6. El servidor vuelve a comprobar la coincidencia y el estado al guardar para evitar decisiones basadas en información desactualizada.

El checklist diario siempre queda como un registro separado y completo. Solo comparte la falla principal con otras constataciones.

## Modelo de datos

La relación `RespuestaHigiene.reporte_falla` debe admitir varias respuestas para un mismo reporte. El diseño lógico es una relación muchos-a-uno:

```text
ReporteFalla #123
├── RespuestaHigiene 24/09
├── RespuestaHigiene 25/09
└── RespuestaHigiene 26/09
```

La migración cambiará la relación actual de uno-a-uno a llave foránea nullable y preservará los identificadores existentes. El nombre inverso deberá representar una colección, por ejemplo `constataciones_higiene`, y todos los consumidores del acceso singular actual deberán actualizarse y probarse.

Cada constatación deberá registrar su interpretación respecto de la falla:

- detección inicial;
- continúa igual;
- cambió o empeoró;
- corrección pendiente de validar.

Puede almacenarse en `RespuestaHigiene` o en una entidad de evento estrechamente ligada a ella. La implementación deberá escoger la opción más pequeña que preserve una sola fuente para fecha, autora, observación y evidencia, sin duplicar esos datos.

La bitácora del reporte recibirá un evento legible por cada continuidad, cambio relevante o solicitud de validación. No se modificará el estado del reporte salvo mediante las acciones autorizadas de Mantenimiento.

## Consistencia y concurrencia

La decisión de reutilizar o crear se ejecutará dentro de la misma transacción que guarda la respuesta del checklist.

- El servidor reconsulta la falla activa antes de enlazar.
- Dos confirmaciones simultáneas de la misma falla deben terminar vinculadas al mismo reporte.
- La creación inicial deberá serializarse por identidad de coincidencia para impedir dos principales por una carrera.
- Si el reporte fue cerrado después de mostrar la pantalla, la captura se tratará como reincidencia y generará un reporte nuevo.
- Si la identidad ya no coincide, se rechazará el enlace y se conservará la captura para reintento sin pérdida de campos.

Las notificaciones y correos se ejecutarán después de confirmar la transacción. Un fallo del canal de aviso no revertirá ni perderá el checklist o su relación con la falla.

## Experiencia de Mantenimiento

La bandeja mostrará una tarjeta por falla principal activa. Cada tarjeta podrá incluir:

- fecha de primera detección;
- fecha de última confirmación;
- días abierta;
- número de constataciones;
- indicador de cambio o empeoramiento;
- acceso a la línea de tiempo completa.

La línea de tiempo combinará, sin confundirlos, eventos de estado de Mantenimiento y constataciones de los checklists. Cada entrada mostrará fecha, persona, sucursal, punto revisado, observación y evidencia disponible.

## Notificaciones

La detección inicial crea la notificación de nueva falla. `Sigue igual` no crea otra tarjeta diaria.

Se genera un nuevo aviso cuando:

- la sucursal indica que la condición cambió o empeoró;
- aumenta la prioridad;
- se solicita validar una corrección;
- se cumple un umbral de antigüedad definido para escalamiento;
- una falla cerrada reaparece como reincidencia.

La bandeja de notificaciones agrupará los avisos históricos por falla principal. El contador visible representará grupos de incidencias activas no leídas, no el número bruto de tarjetas repetidas. Los registros subyacentes se conservarán para auditoría. Abrir el grupo permitirá consultar su historial y aplicar la lectura al conjunto mostrado.

## Consolidación histórica

La consolidación se divide obligatoriamente en vista previa y aplicación.

### Vista previa

Un proceso de solo lectura propondrá grupos usando identidad estructurada y la cronología de estados. Para cada grupo mostrará:

- reporte principal propuesto;
- reportes repetidos propuestos;
- sucursal, checklist, punto, equipo o área y categoría;
- fechas y estados;
- fotografías y autores disponibles;
- motivo de la coincidencia;
- alertas que requieren revisión manual.

El principal será el primer reporte válido del mismo ciclo abierto. Un reporte posterior al cierre será reincidencia y encabezará otro ciclo.

### Aplicación

Solo los grupos exactos y aprobados se aplicarán. Los reportes secundarios se vincularán al principal mediante `duplicado_de`, reutilizando las reglas existentes de duplicados y dejando eventos de auditoría en ambas bitácoras.

La aplicación:

- no eliminará reportes ni notificaciones;
- no cambiará autores, fotografías, descripciones ni folios;
- no reabrirá ni cerrará estados;
- será idempotente: repetirla no creará enlaces o eventos adicionales;
- producirá un resumen de procesados, omitidos y ambiguos.

Los casos con equipo, área, categoría, síntoma o cronología incompatibles quedarán sin cambio para revisión manual.

## Manejo de errores

- Un error de validación conserva las respuestas y archivos seleccionados cuando el navegador lo permita y habilita el reintento.
- Una coincidencia ambigua nunca se fusiona automáticamente.
- Un reporte cerrado no acepta nuevas constataciones de continuidad.
- Un problema en correo o notificaciones queda registrado para reintento, pero no invalida la captura confirmada.
- La consolidación histórica se detiene ante una inconsistencia del grupo actual sin revertir grupos anteriores ya confirmados; el resumen identifica exactamente dónde reanudar.

## Permisos y auditoría

Las sucursales solo podrán enlazar constataciones de su propia sucursal. La selección de una falla principal se validará en el servidor y respetará los permisos actuales.

Cada decisión conservará:

- persona y fecha;
- acción elegida;
- reporte principal;
- respuesta diaria de origen;
- cambios relevantes de clasificación;
- resultado de la notificación.

La consolidación histórica requerirá permisos de administración o Dirección General y registrará actor, fecha, criterio y resultado.

## Pruebas de aceptación

1. Un `No cumple` inicial crea una falla y una notificación.
2. La misma identidad al día siguiente propone la falla activa existente.
3. `Sigue igual` crea otra respuesta diaria, la enlaza al mismo reporte y no crea otra notificación de nueva falla.
4. Una foto opcional de continuidad queda disponible en la línea de tiempo.
5. `Empeoró o cambió` exige comentario y evidencia y genera un aviso relevante.
6. `Es otro problema` crea un reporte independiente.
7. `Ya quedó corregido` no cierra la falla y solicita validación.
8. Dos envíos simultáneos no crean dos principales.
9. Una falla cerrada que reaparece crea una reincidencia nueva.
10. El usuario no puede enlazar respuestas a fallas de otra sucursal.
11. El contador de notificaciones agrupa repeticiones sin borrar los registros originales.
12. La vista previa histórica no escribe datos.
13. La aplicación histórica conserva folios, estados, fotografías, autores y descripciones.
14. Ejecutar dos veces la consolidación no duplica vínculos ni bitácoras.
15. Los casos ambiguos permanecen sin modificar y aparecen en el resumen.

## Despliegue y validación

La implementación deberá separarse en pasos verificables:

1. migración y compatibilidad de la relación muchos-a-uno;
2. coincidencia y captura diaria;
3. línea de tiempo y bandeja de Mantenimiento;
4. agrupación de notificaciones;
5. vista previa histórica;
6. aplicación controlada de grupos aprobados.

Antes de consolidar producción se validará el flujo completo en PostgreSQL local con datos representativos. En producción se desplegará primero el soporte de lectura y captura futura. La consolidación histórica se ejecutará después, comenzando por la vista previa y comparando conteos antes y después.

La aceptación final requiere comprobar en navegador real el checklist de una sucursal, la bandeja de Mantenimiento, la línea de tiempo y el contador de notificaciones. Un despliegue o una migración exitosa por sí solos no constituyen cierre.

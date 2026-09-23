# Diseño: umbral y edición legible de horas extra automáticas

## Objetivo

Evitar que unos cuantos minutos posteriores a la salida programada generen solicitudes automáticas de horas extra y permitir que el responsable de autorizar convierta la evidencia del checador en el bloque de tiempo realmente acordado.

La interfaz hablará siempre en horas y minutos —por ejemplo, `30 min`, `1 h` o `1 h 30 min`— y no mostrará fracciones decimales como `0.50 h` o `1.50 h`.

## Regla de negocio aprobada

1. El sistema calcula el tiempo posterior a la salida programada con las reglas vigentes de turno, comida y cobertura por otros registros.
2. Una solicitud automática solo puede existir cuando el saldo automático no cubierto es de **50 minutos o más**.
3. Un saldo de 0 a 49 minutos no crea una solicitud nueva.
4. Si una propuesta automática pendiente baja de 50 minutos al reevaluar sus marcas o su cobertura, se cancela con una nota operativa; no se elimina.
5. Las solicitudes manuales y los registros autorizados, rechazados, pagados o cancelados no se modifican automáticamente por este umbral.
6. La evidencia detectada se conserva con precisión de minutos. El sistema no redondea por su cuenta una decisión económica.
7. Para autorizar, las horas acordadas deben expresarse en bloques de 30 minutos: `30 min`, `1 h`, `1 h 30 min`, `2 h`, etc.
8. Si el valor detectado ya coincide exactamente con un bloque de 30 minutos, se puede autorizar sin editar.
9. Si el valor detectado no coincide con un bloque —por ejemplo, 58 minutos o 1 h 03 min— el botón Autorizar permanece bloqueado hasta que el responsable registre el bloque acordado y un motivo.

## Alternativas consideradas

### A. Umbral y decisión humana auditada — seleccionada

El sistema conserva el tiempo detectado, exige un bloque de 30 minutos antes de autorizar y registra la corrección humana. Distingue evidencia de checador de decisión de pago.

### B. Redondeo automático

El sistema convertiría 58 minutos a 1 hora sin intervención. Se descarta porque convierte una tolerancia técnica en una decisión económica automática.

### C. Ocultar solicitudes pequeñas

Los registros seguirían existiendo y afectando conteos o procesos posteriores. Se descarta porque no corrige la fuente del problema.

## Flujo de generación

El umbral se aplicará en el servicio compartido que calcula el saldo automático, antes de crear, reactivar o actualizar una `HoraExtra` automática.

- `saldo < 50 min`: el saldo automático esperado es cero para efectos de propuesta.
- `saldo >= 50 min`: se conserva el valor detectado, convertido a la precisión decimal interna existente.
- Una propuesta pendiente automática que quede debajo del umbral se mueve a Cancelado con una nota explícita de saldo inferior al mínimo automático.
- La conciliación diaria, la pantalla de RRHH y las APIs deben leer la misma regla; no habrá umbrales duplicados en vistas o plantillas.

El umbral se define con una constante de dominio nombrada en minutos para evitar números mágicos y confusión con horas decimales.

## Edición previa a la autorización

La pantalla `/rrhh/horas-extra/` agregará la acción **Editar horas** a propuestas pendientes cuando el usuario sea el jefe directo asignado o un superusuario, que son los mismos actores autorizados para resolverlas.

La edición se realizará sin perder la posición del registro y mostrará:

- **Tiempo detectado:** evidencia del checador en horas y minutos.
- **Tiempo a autorizar:** dos controles legibles, horas completas y minutos (`00` o `30`).
- **Motivo del ajuste:** texto obligatorio.

El servidor validará, sin depender del navegador, que:

- el registro siga pendiente;
- el usuario conserve permiso para autorizarlo;
- la jefatura siga vigente;
- la cantidad sea positiva y múltiplo de 30 minutos;
- exista un motivo cuando la cantidad difiera de la evidencia;
- la asistencia, empleado y fecha continúen siendo coherentes;
- no haya cambiado la evidencia mientras el usuario editaba.

La edición reutilizará el bloqueo transaccional, la evidencia `ajuste_autorizacion` y la conciliación ya existentes. No se creará un segundo contrato de corrección paralelo al de las APIs de bonos.

## Presentación de tiempo

Se incorporará un único formateador reutilizable para convertir la cantidad interna a texto humano:

| Valor interno | Presentación |
| --- | --- |
| `0.02 h` | `1 min` |
| `0.50 h` | `30 min` |
| `0.83 h` | `50 min` |
| `1.00 h` | `1 h` |
| `1.50 h` | `1 h 30 min` |
| `2.00 h` | `2 h` |

La precisión decimal interna se conserva para compatibilidad con nómina, montos y APIs actuales. Solo cambia la presentación al usuario y el contrato de captura del nuevo editor.

## Autorización y auditoría

Al guardar un ajuste se conservarán:

- cantidad detectada;
- cantidad acordada;
- huella de las marcas que sustentaron la decisión;
- motivo;
- usuario y fecha del ajuste.

La nota operativa y el registro de auditoría permitirán reconstruir el cambio. Si las marcas o el saldo cambian después, la huella deja de ser vigente y la autorización se bloquea hasta revisar nuevamente.

El monto de nómina se calcula únicamente con la cantidad final autorizada.

## Regularización de pendientes actuales

Se preparará una operación acotada y auditable con vista previa antes de aplicar:

- seleccionar solo propuestas automáticas pendientes cuyo saldo vigente sea menor de 50 minutos;
- reportar conteo e identificadores antes de modificar;
- cambiar su estado a Cancelado, sin borrarlas;
- añadir el motivo del umbral mínimo;
- registrar la operación en auditoría;
- verificar después que ninguna propuesta automática pendiente quede por debajo de 50 minutos.

No se tocarán solicitudes manuales ni estados autorizados, rechazados, pagados o previamente cancelados.

## Experiencia de usuario y accesibilidad

- La acción Editar horas estará junto a Autorizar y Rechazar.
- En móvil los controles se apilarán y usarán etiquetas completas.
- El botón presionado será el único bloqueado durante el guardado.
- El resultado se comunicará con el toast global y se mantendrá el ancla `#hora-extra-<id>`.
- Los errores conservarán los valores capturados y permitirán reintentar.
- Los avisos no dependerán únicamente del color.
- Autorizar mostrará el valor humano final para evitar aprobar una cantidad distinta por error.

## Alcance técnico previsto

- Servicio compartido de cálculo/conciliación de horas extra.
- Servicio transaccional de autorización y ajuste previo.
- Vista y plantilla de `/rrhh/horas-extra/`.
- Estilos específicos de la pantalla para escritorio y móvil.
- Pruebas unitarias, de permisos, concurrencia, presentación y flujo web.
- Operación de regularización auditable de pendientes existentes.

No se requiere cambiar el modelo ni crear una migración: la evidencia y la auditoría necesarias ya existen.

## Criterios de aceptación

1. Una salida con 49 minutos adicionales no crea solicitud automática.
2. Una salida con exactamente 50 minutos sí crea solicitud y se muestra como `50 min`.
3. Una propuesta de 58 minutos se muestra como `58 min` y no puede autorizarse hasta quedar en un bloque de 30 minutos.
4. El jefe puede ajustarla a `1 h`, registrar un motivo y autorizarla.
5. Un ajuste a 1 h 10 min es rechazado por el servidor.
6. Un ajuste a `1 h 30 min` es aceptado.
7. Un usuario sin permiso no puede editar ni autorizar.
8. Si cambian las marcas después del ajuste, la autorización queda bloqueada.
9. Los pendientes automáticos actuales menores a 50 minutos se cancelan conservando historial.
10. Las cantidades se presentan en horas y minutos en escritorio y móvil, sin fracciones decimales.
11. La navegación, el foco, los toasts y el ancla del registro se conservan después de editar.
12. Nómina usa la cantidad final autorizada y no la evidencia original del checador.

## Riesgos y controles

- **Doble regla:** se evita centralizando el umbral y la conversión de tiempo.
- **Redondeo monetario involuntario:** el sistema nunca elige el bloque; lo hace el autorizador.
- **Carrera entre edición y nuevas marcas:** se conservan bloqueos de jornada y huella de evidencia.
- **Alteración histórica:** la regularización solo cancela pendientes automáticos pequeños; nunca borra ni modifica estados cerrados.
- **Diferencias entre pantallas:** el formateador y el contrato de ajuste serán compartidos por RRHH y las APIs consumidoras.

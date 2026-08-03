# Diseño de solicitudes de compras departamentales

Fecha: 2026-08-03
Estado: diseño funcional aprobado para preparar mockup
Alcance inicial: Johana, Carolina, Paula y Yesenia como responsables de área

## 1. Objetivo

Crear dentro del ERP un flujo de compras departamentales que permita a las responsables de área planear necesidades mensuales y registrar necesidades extraordinarias, dar seguimiento individual a cada artículo y evitar que una compra pendiente quede olvidada.

El sistema debe construir historial suficiente para proyectar gasto, comparar precios, detectar excesos presupuestales y apoyar decisiones de autorización, aplazamiento o financiamiento.

## 2. Límites del alcance

Este flujo es exclusivamente para adquisiciones departamentales, por ejemplo:

- herramientas, espátulas, moldes y equipo de trabajo;
- racks, mobiliario, refrigeradores y aires acondicionados;
- uniformes;
- papelería;
- tecnología y equipos para impresión de etiquetas;
- regalos, placas y artículos de reconocimiento para el personal;
- cualquier otra compra de bienes o servicios requerida por un área.

Quedan fuera:

- abastecimiento de producción, recetas, MRP e insumos operativos;
- reportes de fallas y su proceso de atención;
- pagos en efectivo, bonos, compensaciones y cualquier movimiento de nómina;
- modificación de datos operativos o financieros de producción.

Una compra puede coincidir materialmente con un equipo que también podría tener mantenimiento, pero la solicitud de adquisición no se convertirá en un reporte de falla ni dependerá de uno.

## 3. Terminología

### Solicitud departamental

Documento mediante el cual una responsable de área comunica una necesidad. Tiene encabezado y uno o varios artículos. No exige seleccionar proveedor al capturarla.

### Artículo solicitado

Renglón independiente dentro de una solicitud. Conserva su propia imagen, cantidad, costo, prioridad, fecha requerida, cotizaciones, estado, siguiente responsable y recepción.

### Orden de compra

Documento comercial que Compras genera después de cotizar y seleccionar proveedor. Una solicitud departamental puede producir varias órdenes, y una orden puede agrupar artículos compatibles destinados al mismo proveedor.

### Compra mensual y extraordinaria

- **Mensual:** solicitud consolidada que cada responsable prepara del día 20 al 25 para el mes siguiente.
- **Extraordinaria:** solicitud registrada fuera del ciclo normal debido a una necesidad no prevista. Requiere justificación.
- **Emergencia:** variante extraordinaria con prioridad visual alta. No omite revisión presupuestal ni autorización de Dirección General cuando exista exceso.

## 4. Roles y permisos

### Responsable de área

- Crea solicitudes únicamente para su área.
- Guarda y edita borradores.
- Envía solicitudes mensuales o extraordinarias a Compras.
- Consulta cotizaciones, órdenes y avance.
- Responde comentarios o solicitudes de información.
- Confirma la recepción conforme o registra diferencias.
- No autoriza sus propios excesos presupuestales.

El modelo de permisos será extensible: el alcance inicial identifica a Johana, Carolina, Paula y Yesenia, pero no se codificarán nombres como regla permanente.

### Compras

- Accede a una bandeja compartida.
- Asigna un comprador responsable por solicitud o artículo.
- Registra cotizaciones y documentos adjuntos.
- Captura proveedor, costo unitario, descuentos, impuestos y cargos adicionales.
- Selecciona la propuesta recomendada.
- Genera órdenes por proveedor.
- Registra compra, entrega estimada y recepciones parciales.
- Conserva visibles los pendientes hasta su cierre real.

### Dirección General

- Recibe únicamente decisiones que exceden presupuesto.
- Consulta monto, impacto, cotizaciones, necesidad y antecedentes.
- Autoriza, rechaza, pospone o envía a evaluación de financiamiento.
- Deja comentario obligatorio en decisiones distintas de autorizar.

### Administración financiera

- Mantiene la relación entre categoría de compra, área, centro de costo y presupuesto.
- Confirma la clasificación financiera sugerida cuando sea necesario.
- No sustituye la decisión de Dirección General ante un exceso.

## 5. Flujo principal

1. La responsable crea una solicitud mensual o extraordinaria.
2. Agrega uno o varios artículos y guarda borradores sin afectar presupuesto.
3. Envía la solicitud a la bandeja compartida de Compras.
4. Compras asigna responsable y cotiza cada artículo.
5. Compras selecciona proveedor y propuesta.
6. El ERP evalúa el costo seleccionado contra el presupuesto del área y periodo.
7. Si no hay exceso, el artículo queda disponible para generar orden.
8. Si existe exceso, pasa a Dirección General.
9. Dirección General autoriza, rechaza, pospone o solicita evaluación de financiamiento.
10. Compras genera una o varias órdenes, agrupadas por proveedor.
11. Compras registra avance, compra y entregas parciales.
12. La responsable confirma la recepción conforme de cada artículo.
13. La solicitud se completa cuando todos sus artículos llegan a un estado terminal.

Una solicitud se muestra como **Parcialmente atendida** mientras convivan artículos completados y pendientes.

## 6. Estados y responsabilidad de la siguiente acción

### Estado de solicitud

- Borrador
- Enviada a Compras
- En atención
- Parcialmente atendida
- Completada
- Cancelada

### Estado de artículo

- Por revisar
- Por cotizar
- Cotizando
- Esperando información del área
- Esperando autorización de Dirección General
- Pospuesto
- Evaluando financiamiento
- Autorizado
- Ordenado
- Recibido parcialmente
- Comprado, pendiente de confirmación
- Recibido conforme
- Rechazado
- Cancelado

Todo artículo activo tendrá:

- responsable de la siguiente acción;
- fecha compromiso;
- fecha del último movimiento;
- indicador de antigüedad;
- comentario o evidencia más reciente.

Los estados rechazado y cancelado son terminales y requieren motivo. Un registro enviado no se elimina: se cancela preservando el historial.

## 7. Datos de la solicitud y sus artículos

### Encabezado

- folio;
- área y responsable;
- tipo: mensual, extraordinaria o emergencia;
- mes de planeación;
- fecha de creación y envío;
- motivo general;
- estado y avance total;
- total estimado, cotizado, comprometido y recibido;
- comprador asignado;
- historial de eventos.

### Artículo

- imagen opcional de referencia;
- descripción clara;
- categoría sugerida;
- cantidad y unidad;
- fecha requerida;
- prioridad;
- consecuencia de posponer, cuando aplique;
- costo unitario estimado opcional;
- subtotal estimado calculado;
- justificación particular opcional;
- archivos o enlaces de referencia;
- estado y siguiente responsable.

La imagen podrá subirse desde archivo o cámara, verse como miniatura y ampliarse. Seguirá visible en la solicitud, la bandeja de Compras, la comparación de cotizaciones, la autorización y la recepción.

### Cotización

- proveedor;
- documento adjunto;
- vigencia;
- cantidad ofrecida;
- costo unitario;
- descuento;
- subtotal;
- impuestos;
- envío, instalación y otros cargos;
- total de adquisición;
- costo efectivo por unidad;
- tiempo de entrega;
- garantía u observaciones;
- indicador de propuesta seleccionada.

El costo unitario cotizado y el proveedor son obligatorios antes de generar una orden. El costo unitario estimado por el área es opcional.

## 8. Control financiero

El sistema distinguirá sin mezclarlos:

1. **Solicitado:** estimación del área; sirve para proyección.
2. **Cotizado:** monto ofrecido por proveedores.
3. **Comprometido:** propuesta autorizada y convertida en orden.
4. **Gastado:** compra registrada por la fuente financiera correspondiente.

Una solicitud u orden no se contabilizará automáticamente como gasto real. Los tableros mostrarán los compromisos por separado para evitar doble conteo.

Para cada decisión se presentarán:

- presupuesto del área para el periodo;
- gasto real acumulado;
- compromisos vigentes;
- disponible antes de la compra;
- costo total de adquisición seleccionado;
- disponible proyectado después de la compra;
- monto exacto del exceso, si existe.

La regla de exceso será explícita:

`disponible proyectado = presupuesto - gastado - otros compromisos activos - compra seleccionada`

Si el disponible proyectado es menor que cero, la compra requiere decisión de Dirección General. Un compromiso cancelado o rechazado deja de reservar presupuesto. El mismo artículo no puede generar dos compromisos activos por reintentos o por aparecer en más de una vista.

La comparación de cotizaciones usará costo total de adquisición y costo efectivo unitario, no solo el precio de lista. El historial permitirá advertir variaciones contra compras anteriores y oportunidades de consolidación entre áreas.

## 9. Pantallas del mockup

### 9.1 Inicio de la responsable

- tarjeta del ciclo mensual con ventana del 20 al 25;
- aviso si la solicitud del mes siguiente aún no fue enviada;
- botón para preparar solicitud mensual;
- botón para solicitud extraordinaria;
- resumen de borradores, cotizaciones, pendientes y completadas;
- tabla de solicitudes recientes con avance por artículos.

### 9.2 Captura de solicitud

- encabezado simple con área autocompletada;
- tabla editable de artículos;
- primera columna para imagen o cámara;
- cálculo automático de subtotal estimado;
- agregar, duplicar y retirar renglón;
- guardar borrador y enviar a Compras;
- errores junto al campo correspondiente sin perder datos.

### 9.3 Bandeja compartida de Compras

- contadores de nuevas, sin asignar, cotizando, esperando autorización, en compra, vencidas y antiguas;
- tabla filtrable por área, comprador, estado, prioridad, fecha y antigüedad;
- miniatura del artículo;
- acción rápida para asignar comprador;
- responsable y fecha de la siguiente acción siempre visibles;
- actualización de artículos sin perder filtros o posición.

La experiencia principal será una tabla operativa clara; no se usará un tablero Kanban pesado como vista predeterminada.

### 9.4 Detalle y seguimiento

- resumen de la solicitud;
- avance por artículo;
- imágenes ampliables;
- cotizaciones comparables;
- impacto presupuestal;
- órdenes generadas;
- entregas parciales;
- comentarios, adjuntos e historial.

### 9.5 Bandeja de Dirección General

- únicamente compras con exceso presupuestal;
- monto y porcentaje del exceso;
- fotografía, descripción y necesidad;
- propuesta recomendada y alternativas;
- presupuesto, gasto, compromiso y disponible;
- acciones autorizar, posponer, evaluar financiamiento o rechazar.

## 10. Seguimiento y recordatorios

- Una bandeja interna mantendrá todos los artículos activos.
- Los pendientes sin movimiento se destacarán por antigüedad.
- Compras recibirá un resumen semanal de pendientes y entregas vencidas.
- Las responsables verán cuándo la siguiente acción corresponde a su área.
- Las solicitudes mensuales faltantes se recordarán durante la ventana del 20 al 25.
- Los recordatorios externos por correo o mensajería quedan fuera del primer mockup.

## 11. Recepción y cierre

Compras registra que el artículo fue comprado o enviado. La responsable del área debe confirmar:

- recibido conforme;
- recibido parcialmente;
- producto incorrecto;
- producto dañado;
- no recibido.

Una diferencia mantiene el artículo abierto y devuelve la siguiente acción a Compras. Registrar una diferencia de recepción no crea ni mezcla un reporte de falla.

## 12. Comportamiento ante errores

- Guardar o enviar bloquea únicamente el botón accionado y previene doble envío.
- Los datos capturados se conservan ante error.
- Las validaciones se muestran en el renglón y campo correspondientes.
- Los archivos inválidos se rechazan sin perder el resto de la solicitud.
- Los cambios concurrentes se detectan antes de sobrescribir cotizaciones, estados o decisiones.
- Las transiciones inválidas se rechazan con explicación y se conserva el estado anterior.
- Toda acción exitosa o fallida informa mediante el toast global accesible.

## 13. Dirección visual

- Interfaz cálida, profesional y coherente con Pollyana's Dolce.
- Vino para navegación y acciones principales; dorado para impacto financiero y avisos importantes.
- Nunito para interfaz y Playfair Display únicamente en encabezados relevantes.
- Miniaturas visuales claras, con ampliación accesible.
- Estados expresados con texto e icono además de color.
- Densidad moderada: suficiente información para operar sin parecer una suite empresarial genérica.
- Escritorio como superficie principal y adaptación móvil útil para capturar fotografías y consultar seguimiento.
- Contraste, foco visible y navegación por teclado con objetivo WCAG AA.

## 14. Arquitectura conceptual y compatibilidad

El nuevo flujo se modelará como un agregado independiente:

`Solicitud departamental -> artículos -> cotizaciones -> decisiones -> órdenes por proveedor -> recepciones`

La separación es deliberada porque la solicitud existente de Compras representa un solo insumo y participa en flujos de producción. El diseño no convertirá esa solicitud de insumo en un documento departamental genérico ni cambiará el origen canónico de insumos.

La futura implementación deberá:

- crear un encabezado departamental y renglones independientes;
- relacionar cada orden con uno o varios renglones de la solicitud original;
- conservar la orden comercial por proveedor;
- registrar recepción y diferencias por renglón;
- consultar presupuesto mediante un servicio financiero compartido;
- publicar compromisos como una capa separada del gasto real;
- preservar sin cambios los consumidores actuales de solicitudes de insumos, MRP y producción.

Las imágenes y cotizaciones serán adjuntos con validación de tipo, tamaño y autorización de acceso. Los usuarios solo podrán descargar archivos pertenecientes a solicitudes dentro de su alcance.

## 15. Criterios de aceptación del mockup

El mockup deberá demostrar, con datos ficticios:

1. Captura mensual con varios artículos e imágenes opcionales.
2. Captura extraordinaria con justificación.
3. Cálculo de costo unitario y subtotal estimado.
4. Bandeja compartida con asignación de comprador.
5. Tres cotizaciones comparables por costo total y unitario efectivo.
6. Una solicitud dividida en órdenes para proveedores distintos.
7. Un artículo que excede presupuesto y pasa a Dirección General.
8. Las cuatro decisiones de Dirección General.
9. Compra y recepción parciales sin ocultar pendientes.
10. Confirmación final por la responsable del área.
11. Historial completo y responsable de la siguiente acción.
12. Diferencia de recepción que permanece dentro de Compras y no crea un reporte de falla.

## 16. Fuera del primer mockup

- Integración contable automática definitiva.
- Recomendaciones predictivas entrenadas con datos reales.
- Solicitud automática de cotizaciones a proveedores.
- Notificaciones por WhatsApp, correo o aplicaciones externas.
- Financiamiento contratado o gestión bancaria.
- Migraciones, cambios de producción y carga de datos reales.

El mockup debe dejar visibles los puntos donde estas capacidades se conectarían después, sin simular que ya existen.

# Domicilios con fuente única: Point, ERP y Centro Operativo

Fecha: 2026-07-27
Estado: diseño aprobado por el usuario
Repositorios involucrados:

- ERP: `/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1`
- E-commerce y Centro Operativo: `/Users/mauricioburgos/Downloads/Pollyana's Dolce e-commerce/pollyanas-ecommerce`

## 1. Objetivo

Concentrar en un solo flujo los pedidos a domicilio originados en tienda en línea, teléfono, WhatsApp, Facebook, Instagram, mostrador u otro canal. Cada domicilio debe estar ligado a una nota real de Point, a un cliente y dirección reutilizables, y a la asignación de repartidor y unidad del ERP.

El ERP será la única fuente operativa de clientes, direcciones, pedidos, domicilios, asignaciones, estados y auditoría. Centro Operativo será una interfaz responsive que lee y escribe esos mismos registros mediante la API del ERP. Point seguirá siendo la fuente comercial de la venta.

## 2. Evidencia verificada

La revisión de producción fue de solo lectura.

- `/Report/NotasByPlaza` devolvió 48,196 notas para los 120 días revisados.
- Cada nota incluyó `PK_NOTA`, `FOLIO`, `SUCURSAL`, `DIA`, `HORA`, `MONTO`, `FACTURADO`, `CANAL_VENTA`, `TIPO`, `PLAZA` y `JORNADA`.
- `PK_NOTA` fue único. La combinación sucursal + folio también fue única en la ventana revisada.
- El reporte de encabezados no incluyó el detalle de productos.
- Point tenía cuatro productos relacionados con domicilio: `SERVICIO A DOMICILIO`, `Servicio Domicilio 1`, `Servicio Domicilio 2` y `Servicio Domicilio 3`.
- En la ventana de 120 días, Point registró ventas de cargos de domicilio en Matriz y Las Glorias.
- `CANAL_VENTA` solo distinguió `Mostrador` y `Venta en Línea`; no separó teléfono, WhatsApp, Facebook o Instagram.
- El ERP ya contiene modelos para `Cliente`, `DireccionCliente`, `PedidoCliente` y `SolicitudDomicilio`, además de restricciones idempotentes para orígenes externos.
- En el momento de la revisión, la base productiva del CRM tenía 0 clientes, 0 direcciones y 0 pedidos. La migración no debe asumir que ya existe una base consolidada de clientes de Point.
- La pantalla actual de Centro Operativo crea un pedido omnicanal simple y muestra un resumen de domicilios, pero dirige a una bandeja ERP separada mediante “Gestionar en ERP”.
- El ERP conserva una vista distinta para asignar pedidos de la tienda en línea a repartidor y unidad. Esa vista crea registros de entrega separados y no constituye todavía una bandeja omnicanal única.

## 3. Decisión arquitectónica

### 3.1 Propiedad de datos

Point es dueño de:

- `PK_NOTA` y folio;
- sucursal, fecha y hora;
- productos, cantidades, precios y descuentos;
- total, impuestos, forma o estado de pago;
- estado de facturación.

ERP es dueño de:

- cliente y teléfonos;
- direcciones, referencias y coordenadas GPS;
- canal real de contacto;
- vínculo único con la nota de Point;
- ventana e instrucciones de entrega;
- estado operativo;
- repartidor y unidad;
- incidencias, seguimiento y auditoría.

Centro Operativo no será dueño de datos. Será una interfaz del ERP orientada a call center y operación móvil.

### 3.2 Identidad e idempotencia

La llave técnica de la venta será `POINT_NOTE:<PK_NOTA>`. El folio será el dato visible que captura la operadora.

- Una `PK_NOTA` solo podrá ligarse a un `PedidoCliente`.
- Repetir una solicitud con la misma llave devolverá el pedido existente.
- Si el folio ya está vinculado, Centro Operativo abrirá la ficha existente.
- Los datos comerciales recuperados de Point se guardarán como snapshot inmutable y con fecha de consulta.
- Un reintento no podrá duplicar cliente, dirección, pedido, domicilio, asignación ni evento de auditoría.

### 3.3 Integración entre repositorios

El ERP expondrá las API canónicas. Centro Operativo no consultará Point directamente y no escribirá una base paralela.

Flujo:

1. Centro Operativo solicita al ERP buscar una nota.
2. ERP consulta Point o su réplica canónica.
3. ERP devuelve candidatos normalizados.
4. La operadora confirma la nota correcta.
5. ERP crea o reutiliza cliente y dirección.
6. ERP crea el pedido y domicilio dentro de una transacción.
7. Centro Operativo muestra el registro devuelto por el ERP.
8. Cambios posteriores de cualquier interfaz se aplican al mismo registro.

## 4. Consulta y vinculación con Point

### 4.1 Fase obligatoria de descubrimiento

Antes de construir el formulario final se debe identificar y probar el endpoint o reporte de Point que entregue el detalle de una `PK_NOTA`. La implementación no puede inferir los productos comparando totales agregados ni reconstruirlos desde ventas diarias.

La fase se considera exitosa únicamente si, para una nota real de prueba, se recuperan:

- `PK_NOTA`;
- folio;
- sucursal;
- fecha y hora;
- líneas de producto con código, descripción, cantidad, precio y descuento;
- total;
- pago y facturación disponibles.

Si Point no expone un endpoint utilizable, se implementará un adaptador de importación de reporte oficial por nota. No se aceptará capturar productos o total manualmente como sustituto silencioso.

### 4.2 Búsqueda

La captura principal pedirá:

- folio;
- sucursal.

Fecha será un filtro de refinamiento cuando haya resultados ambiguos. El sistema mostrará candidatos con sucursal, fecha, hora y total antes de vincular.

El folio de factura podrá utilizarse únicamente después de comprobar una relación verificable con `PK_NOTA`. Hasta entonces no se presentará como búsqueda garantizada.

### 4.3 Nota no encontrada o Point no disponible

Se permitirá un borrador `PENDIENTE_POINT` con:

- canal;
- cliente;
- teléfono;
- dirección;
- referencias;
- fecha solicitada;
- notas operativas.

El borrador no tendrá total validado ni productos confirmados, no podrá marcarse listo ni asignarse a reparto y mostrará una alerta persistente. Un proceso de conciliación volverá a intentar la vinculación.

## 5. Cliente y dirección

### 5.1 Dedupe de clientes

La búsqueda principal será por teléfono normalizado. El correo será señal secundaria. Antes de crear un cliente se mostrarán coincidencias y direcciones activas.

No se fusionarán automáticamente personas distintas por coincidencia de nombre. Las fusiones manuales conservarán auditoría.

### 5.2 Base de clientes de Point

La importación de clientes desde Point solo se realizará si Point expone identificador, teléfono y datos de contacto con calidad suficiente. Los clientes no se deducirán de ventas agregadas ni de nombres incompletos.

Si Point no ofrece esa base, el ERP crecerá de forma progresiva con cada domicilio, reutilizando clientes por teléfono. Esta limitación deberá mostrarse en el plan de migración.

### 5.3 Direcciones

Cada cliente podrá tener varias direcciones con:

- alias;
- dirección;
- referencias;
- latitud y longitud;
- `place_id`;
- dirección predeterminada;
- estado activo.

Una dirección se reutilizará por su forma normalizada. Latitud y longitud siempre se guardarán juntas. La operadora confirmará el punto en mapa antes de asignar reparto.

## 6. Canales

Los canales canónicos serán:

- `WEB`;
- `TELEFONO`;
- `WHATSAPP`;
- `FACEBOOK`;
- `INSTAGRAM`;
- `MOSTRADOR`;
- `OTRO`.

Facebook e Instagram admitirán referencia opcional de perfil, conversación o mensaje. Este dato será operativo y no se enviará al repartidor.

El canal `OTRO` exigirá descripción. Los valores actuales del ERP se migrarán de forma explícita; `OTRO` no será un depósito automático para Facebook o Instagram.

## 7. Flujo de captura

### Paso 1: buscar la venta

La operadora captura folio y sucursal. ERP busca en Point y devuelve candidatos.

### Paso 2: confirmar la nota

La interfaz muestra productos, cantidades, descuentos, total, sucursal, fecha y pago. Los datos de Point no son editables.

### Paso 3: encontrar al cliente

La operadora busca por teléfono. Selecciona un cliente existente o crea uno nuevo.

### Paso 4: canal y dirección

La operadora selecciona canal, perfil social opcional y una dirección existente o nueva. Confirma GPS y referencias.

### Paso 5: preparar el domicilio

Captura ventana de entrega, instrucciones y observaciones. Guarda el pedido.

### Paso 6: asignar y seguir

Selecciona repartidor y unidad activos del ERP. El pedido pasa por los estados canónicos y el repartidor recibe la información necesaria.

## 8. Estados

Estados canónicos:

- `PENDIENTE_POINT`: borrador sin nota confirmada;
- `CONFIRMADO`: nota ligada;
- `PREPARANDO`;
- `LISTO`;
- `EN_RUTA`;
- `ENTREGADO`;
- `CANCELADO`.

Transiciones:

- `PENDIENTE_POINT → CONFIRMADO` requiere nota de Point.
- `CONFIRMADO → PREPARANDO → LISTO` corresponde a preparación.
- `LISTO → EN_RUTA` requiere repartidor y unidad activos.
- `EN_RUTA → ENTREGADO` requiere evidencia de entrega.
- `CANCELADO` exige motivo y conserva historial.

No se utilizarán estados de bitácoras antiguas como segunda máquina de estados.

## 9. Bandejas y accesos

### 9.1 Centro Operativo

Será la interfaz principal de call center:

- nuevo domicilio;
- búsqueda y seguimiento;
- filtros por estado, canal, sucursal, fecha y repartidor;
- alertas por nota pendiente, GPS faltante, pedido listo sin repartidor e incidencia;
- ficha completa.

### 9.2 ERP

Usará los mismos pedidos para:

- administración de permisos;
- repartidores y unidades;
- auditoría;
- conciliación con Point;
- reportes;
- resolución de excepciones.

“Gestionar en ERP” abrirá la URL exacta de la ficha compartida. No abrirá una bandeja antigua ni una vista que vuelva a crear la entrega.

### 9.3 Repartidor

Verá únicamente pedidos asignados y datos mínimos:

- folio operativo;
- productos y cantidades necesarios para comprobar carga;
- total de referencia cuando corresponda;
- nombre y teléfono;
- dirección, referencias y GPS;
- instrucciones;
- estado.

No verá perfil social, notas internas ni datos de otros clientes.

## 10. Consolidación de pantallas duplicadas

Antes de eliminar rutas se elaborará un inventario de:

- rutas accesibles desde navegación;
- rutas existentes sin botón;
- modelos escritos por cada pantalla;
- bitácoras y estados repetidos;
- enlaces externos entre ERP y Centro Operativo.

Cada ruta se clasificará:

- conservar como interfaz del modelo canónico;
- redirigir a la ficha canónica;
- dejar temporalmente en solo lectura;
- retirar después de migración verificada.

No se borrarán historiales. Los registros antiguos conservarán su trazabilidad y quedarán fuera de bandejas activas cuando corresponda.

## 11. Responsive obligatorio

Cada pantalla se construirá mobile-first y se validará en:

- 320 px;
- 375 px;
- 768 px;
- 1024 px;
- 1280 px;
- 1440 px.

Contrato:

- una columna en móvil;
- dos columnas o paneles plegables en tablet;
- bandeja densa y panel de detalle en escritorio;
- sin desplazamiento horizontal accidental;
- campos y tarjetas con ancho fluido;
- botones y controles táctiles de al menos 44 × 44 px;
- tablas convertidas en tarjetas o detalle móvil;
- teclado adecuado para teléfono y campos numéricos;
- estados de carga, error, vacío y sin conexión con estructura estable;
- GPS, cámara, llamada y navegación probados en navegador móvil compatible.

Ninguna historia se considerará terminada sin capturas reales de desktop, tablet y móvil.

### 11.1 Sistema de orden visual con UI/UX Pro Max

La construcción y revisión del frontend utilizarán `ui-ux-pro-max` como guía obligatoria. Sus recomendaciones se aplicarán dentro del sistema visual actual de Pollyana's Dolce; no se sustituirá la marca por una paleta o tipografía genérica.

El patrón elegido será un dashboard operativo de densidad adaptable:

- escritorio: alta visibilidad de datos, filtros persistentes y jerarquía compacta;
- tablet: tarjetas en dos columnas, filtros plegables y acciones táctiles;
- móvil: una columna, una acción primaria por bloque y detalle progresivo;
- encabezados, filtros, tarjetas, formularios, estados y acciones conservarán el mismo orden semántico en todos los tamaños;
- el orden de tabulación coincidirá con el orden visual;
- el color no será el único indicador de estado;
- texto normal con contraste mínimo 4.5:1;
- cuerpo de texto móvil de al menos 16 px y altura de línea entre 1.5 y 1.75;
- etiquetas visibles asociadas a cada campo;
- foco de teclado visible;
- iconografía SVG consistente, sin emojis como iconos;
- estados hover sin desplazamiento de layout y transiciones de 150–300 ms;
- botones asíncronos deshabilitados durante el envío y con retroalimentación de carga;
- mensajes de validación próximos al campo que los originó;
- espacio reservado para contenido asíncrono, evitando saltos al cargar;
- `prefers-reduced-motion` respetado;
- escala de capas definida para navegación, paneles, menús y diálogos;
- imágenes responsivas optimizadas y con texto alternativo cuando comuniquen información.

Antes de entregar cada pantalla se ejecutará la lista de control de `ui-ux-pro-max` para accesibilidad, interacción, contraste, layout y responsive. Las recomendaciones automáticas que contradigan la marca o el flujo aprobado se documentarán y se descartarán de forma explícita.

## 12. Seguridad, fallback y auditoría

- Las llamadas entre Centro Operativo y ERP usarán autenticación de servicio y permisos por rol.
- Las búsquedas de clientes no expondrán listas completas ni datos innecesarios.
- Los endpoints de repartidor devolverán datos mínimos.
- Los snapshots de Point serán inmutables.
- Las asignaciones y transiciones usarán claves idempotentes.
- Los cambios de cliente, dirección, canal, repartidor, unidad y estado generarán auditoría.
- Los fallos de Point o ERP mostrarán estado claro y permitirán reintento seguro.
- Una cola offline no podrá repetir mutaciones ya aplicadas.

## 13. Pruebas y evidencia

### Backend

- búsqueda y normalización de notas;
- snapshot inmutable;
- idempotencia y concurrencia;
- dedupe de cliente y dirección;
- permisos;
- transiciones;
- asignación de repartidor y unidad;
- auditoría;
- fallos de Point.

### Integración

- Point → ERP → Centro Operativo;
- pedido web → ERP;
- pedido telefónico/social → ERP;
- ERP → Centro Operativo;
- asignación → repartidor;
- reintentos y caídas parciales.

### Frontend

- captura completa;
- candidatos de notas;
- cliente existente y nuevo;
- varias direcciones;
- validación GPS;
- estados de error, vacío y carga;
- teclado y controles táctiles.
- revisión `ui-ux-pro-max` de jerarquía, espaciado, accesibilidad, interacción y consistencia;
- navegación completa por teclado y foco visible;
- contraste, reducción de movimiento y ausencia de desplazamiento horizontal.

### Regresión

- checkout y pedidos web existentes;
- panel de pedidos;
- despacho;
- integración de repartidores;
- logística y rutas actuales;
- autenticación y permisos;
- PWA y caché.

### Cierre por fase

Cada fase entregará:

- pruebas automatizadas aprobadas;
- typecheck, lint y build;
- capturas reales en desktop, tablet y móvil;
- verificación de ruta y API;
- prueba post-deploy;
- respaldo y rollback cuando exista migración;
- lista de rutas antiguas redirigidas o retiradas.

## 14. Secuencia de implementación

1. Descubrir y probar detalle de nota en Point.
2. Definir contrato canónico y migraciones del ERP.
3. Implementar búsqueda y vínculo idempotente.
4. Consolidar cliente, dirección y canales.
5. Implementar ficha y bandeja canónicas del ERP.
6. Rehacer captura responsive en Centro Operativo.
7. Conectar asignación y seguimiento del repartidor.
8. Migrar o redirigir pantallas duplicadas.
9. Ejecutar pruebas de integración, regresión y responsive.
10. Desplegar por fases con respaldo, monitorización y validación real.

## 15. Criterios de aceptación

El trabajo estará completo cuando:

- una nota de Point produzca un solo pedido;
- productos y total provengan de Point;
- teléfono encuentre clientes y direcciones existentes;
- Facebook e Instagram sean canales separados;
- ERP y Centro Operativo muestren el mismo registro y estado;
- “Gestionar en ERP” abra la ficha exacta;
- el repartidor reciba dirección GPS y detalle necesario;
- reintentos y doble clic no dupliquen datos;
- fallos externos produzcan borradores o reintentos seguros;
- rutas antiguas no creen registros paralelos;
- desktop, tablet y móvil pasen las pruebas y tengan evidencia visual;
- producción quede verificada sin regresiones en pedidos web, despacho o logística.

## 16. Compatibilidad temporal del contrato de asignación

Durante la coordinación de versión con el BFF, `POST
/api/public/v1/logistica/domicilios/<id>/asignar/` acepta temporalmente el
payload legado con `repartidor_id` y `actor`, sin `unidad_id`. El ERP infiere
exclusivamente la `unidad_asignada` canónica del repartidor cuando está activa,
disponible y pertenece a la misma sucursal. Si esa relación no es válida, la
API falla con `400` y no asigna.

`unidad_id` permanece soportado y es el contrato recomendado para clientes
actualizados. El BFF deberá empezar a enviarlo a partir de su siguiente versión
y coordinar telemetría de payloads legados antes de retirar la inferencia. La
omisión se considera deprecada: no deberá eliminarse hasta confirmar que no hay
clientes activos usando el payload anterior y anunciar el cambio en una versión
mayor del contrato público.

En domicilios terminales no se infiere equivalencia material: repartidor y
unidad deben coincidir exactamente con lo almacenado para responder de forma
idempotente. Cualquier diferencia devuelve conflicto y nunca modifica el
registro terminal.

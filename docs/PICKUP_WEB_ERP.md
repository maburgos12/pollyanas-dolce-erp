# Pickup Web <-> ERP

Integración recomendada para pickup por sucursal usando el ERP como fuente operativa.

## Principio

- La tienda en línea no consulta Point directo.
- La tienda consulta al ERP.
- El ERP responde con disponibilidad publicable por sucursal usando:
  - último snapshot `pos_bridge`
  - reservas activas
  - buffer de seguridad
  - validación de frescura del dato

## Variables operativas ERP

- `PICKUP_AVAILABILITY_FRESHNESS_MINUTES`
- `PICKUP_STOCK_BUFFER_DEFAULT`
- `PICKUP_LOW_STOCK_THRESHOLD`
- `PICKUP_RESERVATION_TTL_MINUTES`

## Endpoints públicos

Todos requieren header:

```http
X-API-Key: <api-key-publica-del-ERP>
```

### 1. Consultar disponibilidad

```http
GET /api/public/v1/pickup-availability/?product_code=01PSV&branch_code=MATRIZ&quantity=1
```

Respuesta ejemplo:

```json
{
  "product_code": "01PSV",
  "product_name": "Pastel Selva Negra",
  "branch_code": "MATRIZ",
  "branch_name": "Matriz",
  "available": true,
  "stock_qty": "5.000",
  "reserved_qty": "0",
  "buffer_qty": "1",
  "available_to_promise": "4.000",
  "requested_qty": "1",
  "status": "AVAILABLE",
  "source": "ERP_POS_BRIDGE",
  "captured_at": "2026-03-18T02:15:00-07:00",
  "snapshot_age_seconds": 420,
  "freshness_seconds": 1200,
  "is_fresh": true
}
```

Estados:

- `AVAILABLE`
- `LOW_STOCK`
- `OUT_OF_STOCK`
- `UNKNOWN`

Regla recomendada en tienda:

- solo permitir pickup si `available=true`
- si `status=UNKNOWN`, mostrar “Sin confirmación de inventario”

### 2. Crear reserva

```http
POST /api/public/v1/pickup-reservations/
Content-Type: application/json
```

Body ejemplo:

```json
{
  "product_code": "01PSV",
  "branch_code": "MATRIZ",
  "quantity": 1,
  "cliente_nombre": "María López",
  "external_reference": "WEB-CART-12345",
  "hold_minutes": 15,
  "notes": "Pickup web checkout"
}
```

Respuesta ejemplo:

```json
{
  "reservation_token": "9ec0d1f8f3d946ff8dcce9c5f1f0abcd",
  "status": "ACTIVE",
  "product_code": "01PSV",
  "product_name": "Pastel Selva Negra",
  "branch_code": "MATRIZ",
  "branch_name": "Matriz",
  "quantity": "1.000",
  "expires_at": "2026-03-18T01:45:00-07:00"
}
```

### 3. Confirmar reserva y crear pedido ERP

```http
POST /api/public/v1/pickup-reservations/{reservation_token}/confirm/
Content-Type: application/json
```

Body ejemplo:

```json
{
  "cliente_nombre": "María López",
  "descripcion": "Pedido web pickup Matriz",
  "monto_estimado": "899.00",
  "fecha_compromiso": "2026-03-18",
  "prioridad": "MEDIA"
}
```

Respuesta ejemplo:

```json
{
  "reservation_token": "9ec0d1f8f3d946ff8dcce9c5f1f0abcd",
  "reservation_status": "CONFIRMED",
  "pedido_id": 145,
  "folio": "PED-202603-0145",
  "cliente": "María López",
  "estatus": "CONFIRMADO",
  "branch_code": "MATRIZ",
  "branch_name": "Matriz"
}
```

### 4. Liberar o cancelar reserva

```http
POST /api/public/v1/pickup-reservations/{reservation_token}/release/
Content-Type: application/json
```

Body ejemplo:

```json
{
  "reason": "Cliente abandonó checkout"
}
```

Respuesta ejemplo:

```json
{
  "reservation_token": "9ec0d1f8f3d946ff8dcce9c5f1f0abcd",
  "status": "RELEASED",
  "refund_required": false
}
```

Si la reserva ya estaba confirmada y ligada a pedido, el ERP la marca `CANCELED`, cancela el pedido CRM y devuelve:

```json
{
  "reservation_token": "9ec0d1f8f3d946ff8dcce9c5f1f0abcd",
  "status": "CANCELED",
  "refund_required": true
}
```

## Flujo recomendado para tienda

1. Cliente elige sucursal.
2. Tienda consulta `pickup-availability`.
3. Si `available=true`, crea reserva temporal.
4. Cliente paga.
5. Tienda confirma la reserva contra el ERP.
6. La tienda dispara sus notificaciones propias.
7. Si el checkout se abandona, la tienda libera la reserva.
8. Si la sucursal cancela por excepción real, la tienda procesa devolución con `refund_required=true`.

## Observaciones

- El ERP no ejecuta reembolso de pasarela por sí mismo porque la pasarela vive en la tienda.
- El ERP sí deja la decisión operativa y el estado transaccional de la reserva/pedido.
- La sucursal no necesita aceptar manualmente cada pedido para que la venta avance.

## Smoke test operativo

Para validar conectividad e integración con la tienda:

```bash
./scripts/smoke_pickup_public_api.sh \
  --product-code 004499 \
  --branch-code MATRIZ
```

Eso valida:

- `GET /api/public/v1/health/`
- `GET /api/public/v1/pickup-availability/`

Para probar reserva y limpieza sin dejar apartados activos:

```bash
./scripts/smoke_pickup_public_api.sh \
  --product-code 004499 \
  --branch-code MATRIZ \
  --mode reserve-release \
  --confirm-live YES
```

Para probar el ciclo completo `reserve -> confirm -> release`:

```bash
./scripts/smoke_pickup_public_api.sh \
  --product-code 004499 \
  --branch-code MATRIZ \
  --mode full-cycle \
  --confirm-live YES
```

Notas:

- `availability` es el modo por default y no tiene efectos reales.
- `reserve-release` crea una reserva real y luego la libera.
- `full-cycle` crea reserva, confirma pedido CRM y luego lo cancela para validar `refund_required=true`.
- Si la disponibilidad regresa `UNKNOWN`, la conectividad esta bien pero el inventario no esta fresco para prometer stock.

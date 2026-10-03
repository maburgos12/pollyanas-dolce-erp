SALES_GROUP = "maya_sales_readonly"
SALES_CAPABILITY = "SALES_READ_ONLY"

ALLOWED_PATHS = {
    "token": frozenset({
        "/api/pos-bridge/products/sale-price/",
        "/api/integraciones/horarios-especiales/effective/",
    }),
    "public_key": frozenset({"/api/public/v1/pickup-availability/"}),
}


def allows_sales_read(kind, method, path):
    return method == "GET" and path in ALLOWED_PATHS.get(kind, frozenset())

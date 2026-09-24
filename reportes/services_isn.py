from decimal import Decimal, ROUND_HALF_UP


CENT = Decimal("0.01")
ZERO = Decimal("0")


def money(value: Decimal) -> Decimal:
    return Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def calcular_isn_sinaloa(base: Decimal) -> Decimal:
    base = max(ZERO, Decimal(base))
    if base <= Decimal("500000.00"):
        return money(base * Decimal("0.024"))
    if base <= Decimal("700000.00"):
        return money(
            Decimal("12000")
            + (base - Decimal("500000.01")) * Decimal("0.026")
        )
    if base <= Decimal("900000.00"):
        return money(
            Decimal("17200")
            + (base - Decimal("700000.01")) * Decimal("0.028")
        )
    return money(
        Decimal("22800") + (base - Decimal("900000.01")) * Decimal("0.03")
    )


def prorratear_isn(
    bases: dict[int, Decimal], total: Decimal
) -> dict[int, Decimal]:
    bases = {key: max(ZERO, Decimal(value)) for key, value in bases.items()}
    denominator = sum(bases.values(), ZERO)
    if denominator <= ZERO:
        raise ValueError("La base gravada total debe ser positiva.")

    exact = {
        key: Decimal(total) * value / denominator for key, value in bases.items()
    }
    rounded = {
        key: value.quantize(CENT, rounding=ROUND_HALF_UP)
        for key, value in exact.items()
    }
    difference = money(Decimal(total) - sum(rounded.values(), ZERO))
    cents = int(abs(difference / CENT))
    direction = CENT if difference > ZERO else -CENT
    order = sorted(
        exact,
        key=lambda key: (
            -(exact[key] - rounded[key]) * (1 if direction > ZERO else -1),
            key,
        ),
    )
    for key in order[:cents]:
        rounded[key] += direction
    return rounded

import base64
from decimal import Decimal

def _decode_numeric(value: str | None, scale: int) -> Decimal | None:
    """base64 (two's-complement big-endian, unscaled) -> Decimal scaled by 10**-scale."""
    if value is None or not isinstance(value, str):
        return value
    bytes = base64.b64decode(value)
    unscaled = int.from_bytes(bytes, byteorder="big", signed=True)
    return Decimal(unscaled).scaleb(-scale)

__all__ = ["_decode_numeric"]
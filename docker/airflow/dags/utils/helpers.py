import base64
import os
import snowflake.connector as conn

from decimal import Decimal
from pathlib import Path
from utils.constants import SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE

# ==========================================
# DATA HELPERS
# ==========================================
def decode_numeric(value: str | None, scale: int) -> Decimal | None:
    """base64 (two's-complement big-endian, unscaled) -> Decimal scaled by 10**-scale."""
    if value is None or not isinstance(value, str):
        return value
    bytes = base64.b64decode(value)
    unscaled = int.from_bytes(bytes, byteorder="big", signed=True)
    return Decimal(unscaled).scaleb(-scale)

def read_file(path: Path, typ: str = 'sql') -> str:
    """Read the contents of a file."""
    if typ == 'sql':
        return path.read_text(encoding="utf-8")
    return "<unsupported type>"

# ==========================================
# SNOWFLAKE HELPERS
# ==========================================
def get_connection():
    return conn.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE
    )
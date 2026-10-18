"""Instrument classification helpers (warrant filter)."""
import re

# 權證 / 牛熊證: 6-char code, 5 digits + digit-or-letter suffix.
# 上市 03xxxx-08xxxx, 上櫃 70xxxx-73xxxx (suffix variants such as 72124U / 73000U).
# Excluded prefixes: 00 (ETF, incl. 00631L/00992A), 01 (REIT 01001T), 02 (ETN), 91 (TDR).
_WARRANT_RE = re.compile(r"^[0-9]{5}[0-9A-Z]$")
_NON_WARRANT_PREFIXES = ("00", "01", "02", "91")
_WARRANT_TYPES = ("權證", "牛熊證", "warrant")


def is_warrant(code, product_type=None) -> bool:
    code = str(code or "").strip().upper()
    if product_type and any(t in str(product_type).lower() for t in _WARRANT_TYPES):
        return True
    return bool(_WARRANT_RE.match(code)) and not code.startswith(_NON_WARRANT_PREFIXES)

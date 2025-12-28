from __future__ import annotations

import re
from typing import Dict, Any, Set

_SLOT_RE = re.compile(r"\{([a-zA-Z0-9_\.]+)\}")

def extract_slots(text: str) -> Set[str]:
    return set(m.group(1) for m in _SLOT_RE.finditer(text or ""))

def fill_slots(text: str, values: Dict[str, Any], keep_unknown: bool = True) -> str:
    def repl(m: re.Match) -> str:
        key = m.group(1)
        if key in values and values[key] is not None:
            return str(values[key])
        return m.group(0) if keep_unknown else ""
    return _SLOT_RE.sub(repl, text or "")


# Minimal helper (paste near imports or into framework/json_utils.py)
import json, os, tempfile
from pathlib import Path
from typing import Any, Dict

def dump_json_atomic(data: Dict[str, Any], path: str, pretty: bool = True, overwrite: bool = True) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {p}")
    text = json.dumps(data, indent=2 if pretty else None, ensure_ascii=False, sort_keys=pretty)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(p.parent), delete=False) as tf:
        tmp = tf.name
        tf.write(text)
    os.replace(tmp, p)  # atomic on same filesystem
    return str(p.resolve())

import os
import json
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional
from glob import glob

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            h.update(data)
    return h.hexdigest()

def write_json(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def now_parts():
    dt = datetime.utcnow()
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H-%M-%S")

def resolve_datalake_root(cli_value: Optional[str]) -> str:
    if cli_value:
        return cli_value
    return os.environ.get("DATA_LAKE_ROOT", "./data_lake")

def _hash_exists(lake_root: str, sha256: str) -> Optional[str]:
    for mf in glob(os.path.join(lake_root, "bronze", "dt_coleta=*","hora=*","manifest.json")):
        try:
            with open(mf, "r") as f:
                if json.load(f).get("sha256") == sha256:
                    return mf
        except Exception:
            pass
    return None

import os
import requests
import re
from typing import Dict, Any, Optional, List
from tenacity import retry, wait_exponential, stop_after_attempt
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CKAN_BASE = os.environ.get("CKAN_BASE_URL", "https://dados.gov.br/dados")
PACKAGE_SEARCH = f"{CKAN_BASE}/api/3/action/package_search"

# Headers: alguns proxies bloqueiam sem UA/Accept explícitos
DEFAULT_HEADERS = {
    "User-Agent": "obs-acessos-q1/1.0 (+https://example.com)",
    "Accept": "application/json",
    "Connection": "keep-alive",
}

def _make_session() -> requests.Session:
    s = requests.Session()
    # Requisições com retry para erros transitórios
    retry_cfg = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_cfg)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
def search_package(query: str) -> Dict[str, Any]:
    """
    Busca pacotes no CKAN. Se a resposta não for JSON, levanta erro com snippet do corpo para debug.
    """
    params = {"q": query, "rows": 20}
    with _make_session() as s:
        r = s.get(PACKAGE_SEARCH, params=params, headers=DEFAULT_HEADERS, timeout=30)
        # Se não vier 200, mostre um pedaço do corpo para entender
        if r.status_code != 200:
            snippet = (r.text or "")[:400].replace("\n", " ")
            raise RuntimeError(f"CKAN {r.status_code} em package_search. Corpo (400c): {snippet}")
        # Conteúdo vazio também é erro
        if not r.text:
            raise RuntimeError("CKAN retornou corpo vazio em package_search.")
        # Tentar decodificar JSON; se falhar, mostrar um trecho do HTML/erro
        try:
            data = r.json()
        except Exception:
            snippet = r.text[:400].replace("\n", " ")
            ct = r.headers.get("Content-Type", "")
            raise RuntimeError(f"Resposta não-JSON (Content-Type={ct}). Trecho (400c): {snippet}")
        if not data.get("success"):
            raise RuntimeError(f"CKAN retornou success=False: {data}")
        return data["result"]

def _score_resource(res: Dict[str, Any]) -> int:
    fmt = (res.get("format") or "").upper()
    if fmt == "CSV": return 100
    if fmt in ("XLSX", "XLS"): return 90
    if fmt == "ZIP": return 80
    return 10

def pick_best_package(result: Dict[str, Any], query: str) -> Optional[Dict[str, Any]]:
    items: List[Dict[str, Any]] = result.get("results", []) or []
    if not items:
        return None
    wanted = query.lower()
    def title_ok(title: str) -> bool:
        t = (title or "").lower()
        return all(w in t for w in ["acessos", "banda", "larga"])
    ranked = sorted(
        items,
        key=lambda x: (title_ok(x.get("title", "")), x.get("metadata_modified", "")),
        reverse=True,
    )
    return ranked[0] if ranked else None

def pick_best_resource(pkg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    resources = pkg.get("resources", []) or []
    if not resources:
        return None
    resources_sorted = sorted(
        resources,
        key=lambda r: (_score_resource(r), r.get("last_modified") or r.get("created") or ""),
        reverse=True,
    )
    return resources_sorted[0]

@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
def download(url: str, dest_path: str) -> dict:
    """
    Faz download e retorna metadados: status_code e original_filename (se existir).
    """
    with _make_session() as s:
        with s.get(url, stream=True, headers=DEFAULT_HEADERS, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            # tentar extrair filename do Content-Disposition
            cd = r.headers.get("Content-Disposition")
            original_filename = None
            if cd:
                # procurar padrão filename="..."
                match = re.search(r'filename\*?="?([^"]+)"?', cd)
                if match:
                    original_filename = match.group(1)

            return {
                "http_status": r.status_code,
                "original_filename": original_filename,
            }
import argparse
import os
from typing import Optional
from datetime import datetime, timezone
from pyspark.sql import SparkSession

from .utils import (
    ensure_dir, write_json, now_parts, resolve_datalake_root, sha256_file,
    _hash_exists,  # você moveu pra utils.py — ótimo!
)
from .ckan_client import search_package, pick_best_package, pick_best_resource, download
from .spark_try import try_csv_rowcount_with_spark

spark = (
    SparkSession.builder
    .appName("Q2_Transform")
    # memória do driver (ajuste pra sua máquina; 6g–10g costuma bem)
    .config("spark.driver.memory", "8g")
    # menos tarefas de shuffle (200 é o default e é alto pra local)
    .config("spark.sql.shuffle.partitions", "64")
    # arquivos menores na escrita → menos buffer na memória
    .config("spark.sql.files.maxRecordsPerFile", "1500000")
    # parquet com dicionário consome mais heap em algumas colunas longas
    .config("parquet.enable.dictionary", "false")
    .getOrCreate()
)

def ingest(dataset_query: str, datalake_root: Optional[str] = None, resource_url: Optional[str] = None) -> str:
    # diretório de saída (bronze)
    root = resolve_datalake_root(datalake_root)
    dt, hr = now_parts()
    out_dir = os.path.join(root, "bronze", f"dt_coleta={dt}", f"hora={hr}")
    ensure_dir(out_dir)

    started_at = datetime.now(timezone.utc).isoformat()

    pkg = None
    res = None
    url = resource_url  # se vier por CLI, pula CKAN

    if not url:
        # fluxo original: busca no CKAN
        result = search_package(dataset_query)
        pkg = pick_best_package(result, dataset_query)
        if not pkg:
            raise SystemExit("Nenhum pacote encontrado no CKAN para a query.")
        res = pick_best_resource(pkg)
        if not res or not res.get("url"):
            raise SystemExit("Pacote encontrado, mas sem resources adequados.")
        url = res["url"]

    # decide extensão de saída
    ext = "csv"
    if res and res.get("format"):
        ext = (res["format"] or "bin").lower()
        if ext not in ("csv", "xlsx", "xls", "zip"):
            ext = "bin"
    else:
        # quando usamos resource_url direto, tentamos inferir da URL
        low = url.lower()
        if low.endswith(".csv"):
            ext = "csv"
        elif low.endswith(".xlsx"):
            ext = "xlsx"
        elif low.endswith(".xls"):
            ext = "xls"
        elif low.endswith(".zip"):
            ext = "zip"
        else:
            ext = "bin"

    raw_path = os.path.join(out_dir, f"raw.{ext}")

    # faz o download e captura metadados http_status e original_filename
    download_meta = download(url, raw_path)

    # calcula o hash do arquivo baixado
    file_hash = sha256_file(raw_path)

    # checa se já existe o mesmo hash no bronze
    duplicate_of = _hash_exists(root, file_hash)

    # contagem com spark é só para CSV
    rowcount_csv_try = None
    if ext == "csv":
        rowcount_csv_try = try_csv_rowcount_with_spark(raw_path)

    finished_at = datetime.now(timezone.utc).isoformat()

    manifest = {
        "dataset_query": dataset_query,
        "ckan_base": os.environ.get("CKAN_BASE_URL", "https://dados.gov.br"),
        "package_title": pkg.get("title") if pkg else None,
        "package_name": pkg.get("name") if pkg else None,
        "package_id": pkg.get("id") if pkg else None,
        "resource_id": res.get("id") if res else None,
        "resource_name": res.get("name") if res else None,
        "resource_format": res.get("format") if res else ext.upper(),
        "resource_url": url,

        "output_dir": out_dir,
        "file_size_bytes": os.path.getsize(raw_path),
        "sha256": file_hash,
        "rowcount_csv_try": rowcount_csv_try,

        # melhorias do passo 2
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "http_status": download_meta.get("http_status"),
        "original_filename": download_meta.get("original_filename"),
    }

    if duplicate_of:
        manifest["duplicate_of"] = duplicate_of

    write_json(os.path.join(out_dir, "manifest.json"), manifest)
    return out_dir


def main():
    parser = argparse.ArgumentParser(description="Q1 - Ingestão Acessos Banda Larga Fixa (dados.gov.br → Data Lake bronze)")
    parser.add_argument("--mode", choices=["ingest"], default="ingest")
    parser.add_argument("--dataset", default="Acessos – Banda Larga Fixa", help="Texto de busca no CKAN (usado se --resource-url não for informado)")
    parser.add_argument("--datalake", default=None, help="Raiz do Data Lake (default: ./data_lake ou env DATA_LAKE_ROOT)")
    parser.add_argument("--resource-url", default=None, help="URL direta do recurso (pula busca no CKAN)")
    args = parser.parse_args()

    if args.mode == "ingest":
        out = ingest(args.dataset, args.datalake, args.resource_url)
        print(f"Ingestão concluída em: {out}")


if __name__ == "__main__":
    main()
import argparse
import os
import re
import json
import zipfile
import logging
from datetime import datetime, timezone
from typing import Optional, List, Tuple

from pyspark.sql import SparkSession, functions as F, types as T
from .utils import ensure_dir, resolve_datalake_root, write_json

logging.basicConfig(level=logging.INFO, format='[Q2] %(message)s')
log = logging.getLogger("Q2")

RES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "resources"))

# yyyy-mm ou yyyy_mm nas colunas "wide"
YEAR_MONTH_RE = re.compile(r"^\d{4}[-_]\d{2}$")


def _read_csv_robust(spark: SparkSession, path: str):
    """
    Tenta ler o CSV variando separador e encoding.
    Devolve o DataFrame que resultar em maior número de colunas (>1).
    """
    candidates = [
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ";", "encoding": "latin1"},
        {"sep": ",", "encoding": "utf-8"},
        {"sep": ",", "encoding": "latin1"},
    ]
    best_df = None
    best_cols = 0
    for opt in candidates:
        try:
            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .option("multiLine", True)
                .option("sep", opt["sep"])
                .option("encoding", opt["encoding"])
                .csv(path)
            )
            ncols = len(df.columns)
            if ncols > best_cols:
                best_cols = ncols
                best_df = df
        except Exception:
            pass
    if best_df is None:
        # última tentativa simples (deixa o erro estourar para debug)
        best_df = spark.read.option("header", True).csv(path)
    return best_df


def _normalize_colname(c: str) -> str:
    s = c.strip().lower()
    s = (
        s.replace("ã", "a")
        .replace("â", "a")
        .replace("á", "a")
        .replace("à", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("è", "e")
        .replace("í", "i")
        .replace("ì", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("õ", "o")
        .replace("ò", "o")
        .replace("ú", "u")
        .replace("ù", "u")
        .replace("ç", "c")
    )
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _find_latest_bronze_dir(lake_root: str) -> Tuple[str, str]:
    bronze = os.path.join(lake_root, "bronze")
    if not os.path.isdir(bronze):
        raise SystemExit("Bronze não encontrado. Rode a Q1 primeiro.")
    dt_dirs = sorted([d for d in os.listdir(bronze) if d.startswith("dt_coleta=")], reverse=True)
    if not dt_dirs:
        raise SystemExit("Nenhum dt_coleta encontrado no bronze.")
    latest_dt = dt_dirs[0]
    base = os.path.join(bronze, latest_dt)
    hr_dirs = sorted([d for d in os.listdir(base) if d.startswith("hora=")], reverse=True)
    if not hr_dirs:
        raise SystemExit("Nenhuma pasta hora= encontrada no bronze.")
    latest_hr = hr_dirs[0]
    return os.path.join(base, latest_hr), f"{latest_dt}/{latest_hr}"


def _extract_zip_if_needed(bronze_dir: str) -> List[str]:
    raw_zip = os.path.join(bronze_dir, "raw.zip")
    extracted = os.path.join(bronze_dir, "extracted")
    csvs: List[str] = []

    if os.path.exists(extracted):
        for root, _, files in os.walk(extracted):
            for f in files:
                if f.lower().endswith(".csv"):
                    csvs.append(os.path.join(root, f))
        if csvs:
            return csvs

    if os.path.exists(raw_zip):
        os.makedirs(extracted, exist_ok=True)
        with zipfile.ZipFile(raw_zip, "r") as z:
            z.extractall(extracted)
        for root, _, files in os.walk(extracted):
            for f in files:
                if f.lower().endswith(".csv"):
                    csvs.append(os.path.join(root, f))
    else:
        raw_csv = os.path.join(bronze_dir, "raw.csv")
        if os.path.exists(raw_csv):
            csvs.append(raw_csv)

    if not csvs:
        raise SystemExit("Nenhum CSV encontrado no bronze (nem dentro do ZIP).")
    return csvs


def _load_dim_uf_regiao(spark: SparkSession):
    dim_path = os.path.join(RES_DIR, "dim_uf_regiao.csv")
    return spark.read.option("header", True).csv(dim_path).select(
        F.upper(F.col("uf")).alias("uf"),
        F.col("regiao"),
    )


def _load_tech_map():
    path = os.path.join(RES_DIR, "tech_map.json")
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    lookup = {}
    for canon, lst in raw.items():
        for v in lst:
            lookup[v.lower()] = canon
    return lookup


def _canonicalize_tech(col: F.Column) -> F.Column:
    tech_map = _load_tech_map()

    @F.udf(T.StringType())
    def map_tech(x):
        if x is None:
            return None
        s = str(x).strip().lower()
        s = (
            s.replace("ã", "a")
            .replace("â", "a")
            .replace("á", "a")
            .replace("à", "a")
            .replace("é", "e")
            .replace("ê", "e")
            .replace("è", "e")
            .replace("í", "i")
            .replace("ì", "i")
            .replace("ó", "o")
            .replace("ô", "o")
            .replace("õ", "o")
            .replace("ò", "o")
            .replace("ú", "u")
            .replace("ù", "u")
            .replace("ç", "c")
        )
        if s in tech_map:
            return tech_map[s]
        if "fibra" in s or "ftth" in s or "fiber" in s:
            return "fibra"
        if "hfc" in s or "cabo" in s or "coax" in s:  # <-- fix: 'or' (não 'ou')
            return "cabo"
        if "dsl" in s:
            return "xDSL"
        if "radio" in s or "wireless" in s or "wifi" in s:
            return "radio"
        if "sat" in s:
            return "satelite"
        if "ethernet" in s or "lan" in s:
            return "lan"
        return s

    return map_tech(col)


def _melt_wide_to_long(df):
    """Converte tabelas 'wide' (colunas 2019-01, 2019_02, ...) para (ano, mes, acessos)."""
    ym_cols = [c for c in df.columns if YEAR_MONTH_RE.match(c)]
    if not ym_cols:
        return None
    parts = []
    for c in ym_cols:
        parts.append(f"'{c}'")
        parts.append(f"`{c}`")
    stack_expr = f"stack({len(ym_cols)}, {', '.join(parts)}) as (ym, acessos)"

    base_cols = [c for c in df.columns if c not in ym_cols]
    keep = [c for c in base_cols if c in ("uf", "tecnologia")]

    melted = (
        df.selectExpr(*([*keep] if keep else ["*"]), stack_expr)
        .withColumn("ym", F.regexp_replace(F.col("ym"), "_", "-"))
        .withColumn("ano", F.split(F.col("ym"), "-").getItem(0).cast("int"))
        .withColumn("mes", F.split(F.col("ym"), "-").getItem(1).cast("int"))
        .drop("ym")
    )
    melted = melted.withColumn(
        "acessos",
        F.regexp_replace(F.col("acessos").cast("string"), r"[^0-9]", "").cast("long"),
    )
    return melted.filter(
        F.col("ano").isNotNull() & F.col("mes").isNotNull() & F.col("acessos").isNotNull()
    )


def _split_competencia(df):
    """
    Lida com colunas únicas tipo 'competencia'/'ano_mes'/'periodo':
    exemplos: '2024-01', '2024_01', '01/2024'.
    Retorna df com colunas ano, mes (ou None se não aplicável).
    """
    for cand in ["competencia", "ano_mes", "periodo", "ano_mes_ref"]:
        if cand in df.columns:
            c = F.regexp_replace(F.col(cand).cast("string"), r"[._]", "-")
            # tenta formatos 2024-01 e 01/2024
            ano = F.when(F.length(c) >= 7, F.split(c.replace("/", "-"), "-").getItem(0)).otherwise(None)
            mes = F.when(F.length(c) >= 7, F.split(c.replace("/", "-"), "-").getItem(1)).otherwise(None)
            df = df.withColumn("ano", ano.cast("int")).withColumn("mes", mes.cast("int"))
            return df
    return None


def _validate_and_coerce_silver(df):
    """
    Valida e ajusta o schema do silver:
      - obriga ano, mes, acessos
      - cria uf/regiao/tecnologia se faltarem (NULL)
      - tipa colunas no formato esperado
    """
    required = ["ano", "mes", "acessos"]
    nice_to_have = ["uf", "regiao", "tecnologia"]

    # checa obrigatórias
    missing_req = [c for c in required if c not in df.columns]
    if missing_req:
        raise SystemExit(f"Silver sem colunas obrigatórias: {missing_req}. Presentes: {df.columns}")

    # cria recomendadas se faltarem
    for c in nice_to_have:
        if c not in df.columns:
            log.warning(f"[WARN] Coluna ausente no silver: '{c}'. Criando como NULL.")
            df = df.withColumn(c, F.lit(None).cast("string"))

    # coerção de tipos
    df = (
        df.withColumn("ano", F.col("ano").cast("int"))
        .withColumn("mes", F.col("mes").cast("int"))
        .withColumn("acessos", F.regexp_replace(F.col("acessos").cast("string"), r"[^0-9]", "").cast("long"))
        .withColumn("uf", F.col("uf").cast("string"))
        .withColumn("regiao", F.col("regiao").cast("string"))
        .withColumn("tecnologia", F.col("tecnologia").cast("string"))
    )

    return df


def transform_q2(datalake_root: Optional[str] = None, output_format: str = "parquet", single_file: bool = False, gold_extra: bool = False):
    lake_root = resolve_datalake_root(datalake_root)
    bronze_dir, _ = _find_latest_bronze_dir(lake_root)
    csv_files = _extract_zip_if_needed(bronze_dir)

    # ---------- Configs de Spark (local) + Adaptive Query Execution ----------
    spark = (
        SparkSession.builder
        .appName("Q2_Transform")
        # memória (ajuste conforme sua máquina)
        .config("spark.driver.memory", "8g")
        # menos partições de shuffle por padrão
        .config("spark.sql.shuffle.partitions", "64")
        # limitar linhas por arquivo de saída
        .config("spark.sql.files.maxRecordsPerFile", "1500000")
        # Parquet: ativar dictionary pages no nível Hadoop (evita warning)
        .config("spark.hadoop.parquet.enable.dictionary", "true")
        # ---------- Adaptive Query Execution (AQE) ----------
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "64")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")
        # permitir broadcast joins quando couber
        .config("spark.sql.autoBroadcastJoinThreshold", "64MB")
        .getOrCreate()
    )

    dfs: List = []
    skipped: List[Tuple[str, List[str]]] = []

    for path in csv_files:
        # LER CSV DE FORMA ROBUSTA (tenta separador ;/, encoding utf-8/latin1)
        df = _read_csv_robust(spark, path)

        # Normaliza nomes das colunas
        for old in df.columns:
            df = df.withColumnRenamed(old, _normalize_colname(old))

        # 1) tenta "wide"
        melted = _melt_wide_to_long(df)
        if melted is not None:
            df_norm = melted
        else:
            # 2) tenta colunas longas usuais
            candidates = {
                "ano": ["ano", "year"],
                "mes": ["mes", "mes_", "mes_ref", "mês", "month"],
                "uf": ["uf", "sigla_uf", "estado", "sigla_estado", "siglauf"],
                "tecnologia": ["tecnologia", "tec", "tecnologia_acesso", "tipo_tecnologia"],
                "acessos": ["acessos", "qtd_acessos", "quantidade_acessos", "total"],
            }

            def find_any(cols, opts):
                for n in opts:
                    if n in cols:
                        return n
                return None

            rename = {}
            for canon, opts in candidates.items():
                found = find_any(df.columns, opts)
                if found and found != canon:
                    rename[found] = canon
            for k, v in rename.items():
                df = df.withColumnRenamed(k, v)

            # 3) se ainda não tem ano/mes, tenta quebrar 'competencia'/'ano_mes'
            if "ano" not in df.columns or "mes" not in df.columns:
                maybe = _split_competencia(df)
                if maybe is not None:
                    df = maybe

            # casts/limpeza
            if "ano" in df.columns:
                df = df.withColumn("ano", F.col("ano").cast("int"))
            if "mes" in df.columns:
                df = df.withColumn("mes", F.col("mes").cast("int"))
            if "acessos" in df.columns:
                df = df.withColumn(
                    "acessos",
                    F.regexp_replace(F.col("acessos").cast("string"), r"[^0-9]", "").cast("long"),
                )

            cols_want = [c for c in ["ano", "mes", "uf", "tecnologia", "acessos"] if c in df.columns]
            df_norm = df.select(*cols_want) if cols_want else None

        # valida mínimas para seguir
        if df_norm is None or not all(c in df_norm.columns for c in ["ano", "mes", "acessos"]):
            # guarda para diagnóstico e pula
            skipped.append((os.path.basename(path), df.columns))
            continue

        # filtros básicos
        df_norm = df_norm.filter(
            F.col("ano").isNotNull() & F.col("mes").isNotNull() & F.col("acessos").isNotNull()
        )
        dfs.append(df_norm)

    # ---------- (2) LOGS DE AUDITORIA DA ENTRADA ----------
    log.info(f"Arquivos CSV encontrados: {len(csv_files)}")
    log.info(f"Arquivos padronizados (viraram DataFrame válido): {len(dfs)}")
    if skipped:
        log.info(f"Arquivos ignorados: {len(skipped)}")
        for name, cols in skipped[:5]:
            log.info(f" - ignorado: {name} | colunas={cols}")

    if not dfs:
        spark.stop()
        msg = "Nenhum arquivo pôde ser padronizado em (ano, mes, acessos).\n"
        if skipped:
            msg += "Exemplos de arquivos ignorados e suas colunas normalizadas:\n"
            for name, cols in skipped[:5]:
                msg += f"- {name}: {cols}\n"
        raise SystemExit(msg.rstrip())

    # union
    silver = dfs[0]
    for d in dfs[1:]:
        silver = silver.unionByName(d, allowMissingColumns=True)

    # canonicaliza tecnologia se existir
    if "tecnologia" in silver.columns:
        silver = silver.withColumn("tecnologia", _canonicalize_tech(F.col("tecnologia")))

    # junta UF→regiao se existir uf
    dim = _load_dim_uf_regiao(spark)
    if "uf" in silver.columns:
        s = silver.alias("s")
        d = dim.alias("d")
        silver = (
            s.withColumn("uf_up", F.upper(F.col("s.uf")))
            .join(d, F.col("uf_up") == F.col("d.uf"), how="left")
            .drop("uf_up")
            .drop(F.col("d.uf"))
        )
    else:
        silver = silver.withColumn("regiao", F.lit(None).cast("string"))

    # >>> VALIDAÇÃO AUTOMÁTICA DO SILVER <<<
    silver = _validate_and_coerce_silver(silver)

    # ---------- (3) AUDITORIA DO SILVER ----------
    silver = silver.persist()
    total_rows = silver.count()
    anos_uniqs = [r["ano"] for r in silver.select("ano").distinct().orderBy("ano").collect()]
    linhas_por_ano = {
        r["ano"]: r["cnt"]
        for r in silver.groupBy("ano").agg(F.count(F.lit(1)).alias("cnt")).orderBy("ano").collect()
    }
    minmax = silver.agg(F.min("ano").alias("min_ano"), F.max("ano").alias("max_ano")).first().asDict()
    # >>> define ano_max para uso nos GOLDs e no manifest <<<
    ano_max = max(anos_uniqs) if anos_uniqs else None
    if ano_max is None:
        spark.stop()
        raise SystemExit("Não foi possível determinar ano máximo (coluna 'ano' vazia).")

    log.info(f"Silver: {total_rows:,} linhas")
    log.info(f"Silver: anos únicos = {anos_uniqs}")
    log.info(f"Silver: linhas por ano = {linhas_por_ano}")
    log.info(f"Silver: faixa de anos = {minmax.get('min_ano')}..{minmax.get('max_ano')}")

    # reordena colunas (estético/padrão)
    cols_order = ["ano", "mes", "uf", "regiao", "tecnologia", "acessos"]
    silver = silver.select(*[c for c in cols_order if c in silver.columns])

    # ---------- liberar memória ANTES de gravar ----------
    silver.unpersist(blocking=True)

    # ---------- grava SILVER ----------
    silver_out = os.path.join(lake_root, "silver", "acessos_blf")
    ensure_dir(silver_out)
    if single_file:
        log.info("Gravando SILVER em único arquivo (coalesce(1)) para conveniência local.")
        (
            silver.coalesce(1)
            .write.mode("overwrite")
            .format(output_format)
            .option("maxRecordsPerFile", "1500000")
            .save(silver_out)
        )
    else:
        log.info("Gravando SILVER por ano (loop) para reduzir shuffle e uso de memória.")
        for y in anos_uniqs:
            log.info(f"  → ano={y}")
            (
                silver.filter(F.col("ano") == y)
                .repartition(4)
                .write.mode("overwrite")
                .format(output_format)
                .option("maxRecordsPerFile", "1500000")
                .save(os.path.join(silver_out, f"ano={y}"))
            )

    # --- GOLD1: total por REGIÃO para todos os anos (não só ano_max) ---
    gold1_base = os.path.join(lake_root, "gold", "q2_total_acessos_por_regiao")
    ensure_dir(gold1_base)

    total_regiao_all = (
        silver.groupBy("ano", "regiao")
        .agg(F.sum("acessos").alias("total_acessos"))
    )

    log.info("Gravando GOLD1 (total por região) por ano (loop) para reduzir shuffle/memória.")
    for y in anos_uniqs:
        log.info(f"  → ano={y}")
        out_y = os.path.join(gold1_base, f"ano={y}")
        (
            total_regiao_all.filter(F.col("ano") == y)
            .drop("ano")  # mantém só regiao, total_acessos dentro da partição ano=Y
            .repartition(1)
            .write.mode("overwrite")
            .format(output_format)
            .option("maxRecordsPerFile", "1000000")
            .save(out_y)
        )

    gold1_out = os.path.join(gold1_base, f"ano={ano_max}")

    # gold 2: evolução por tecnologia (últimos 3 anos)
    anos = [ano_max - 2, ano_max - 1, ano_max]
    evol_tec = (
        silver.filter(F.col("ano").isin(anos))
        .groupBy("ano", "tecnologia")
        .agg(F.sum("acessos").alias("total_acessos"))
        .orderBy("ano", "tecnologia")
    )
    gold2_out = os.path.join(
        lake_root, "gold", "q2_evolucao_por_tecnologia", f"anos={anos[0]}_{anos[-1]}"
    )
    ensure_dir(gold2_out)
    if single_file:
        log.info("Gravando GOLD2 (evolução por tecnologia) em único arquivo.")
        (
            evol_tec.coalesce(1)
            .write.mode("overwrite")
            .format(output_format)
            .option("maxRecordsPerFile", "1000000")
            .save(gold2_out)
        )
    else:
        log.info("Gravando GOLD2 por ano (loop) para reduzir shuffle e uso de memória.")
        for y in anos:
            log.info(f"  → ano={y}")
            (
                evol_tec.filter(F.col("ano") == y)
                .repartition(1)
                .write.mode("overwrite")
                .format(output_format)
                .option("maxRecordsPerFile", "1000000")
                .save(os.path.join(gold2_out, f"ano={y}"))
            )

    # ---------- GOLD EXTRA: evolução por REGIÃO + TECNOLOGIA ----------
    gold3_out = None
    if gold_extra:
        log.info("Gerando GOLD EXTRA: evolução por região + tecnologia (últimos 3 anos).")
        evol_reg_tec = (
            silver.filter(F.col("ano").isin(anos))
            .groupBy("ano", "regiao", "tecnologia")
            .agg(F.sum("acessos").alias("total_acessos"))
            .orderBy("ano", "regiao", "tecnologia")
        )
        gold3_out = os.path.join(
            lake_root, "gold", "q2_evolucao_por_regiao_tecnologia", f"anos={anos[0]}_{anos[-1]}"
        )
        ensure_dir(gold3_out)
        if single_file:
            log.info("Gravando GOLD3 em único arquivo (coalesce(1)).")
            (
                evol_reg_tec.coalesce(1)
                .write.mode("overwrite")
                .format(output_format)
                .option("maxRecordsPerFile", "1000000")
                .save(gold3_out)
            )
        else:
            log.info("Gravando GOLD3 por ano (loop) para reduzir shuffle e uso de memória.")
            for y in anos:
                log.info(f"  → ano={y}")
                (
                    evol_reg_tec.filter(F.col("ano") == y)
                    .repartition(1)
                    .write.mode("overwrite")
                    .format(output_format)
                    .option("maxRecordsPerFile", "1000000")
                    .save(os.path.join(gold3_out, f"ano={y}"))
                )
    
    # ---------- GOLD4: total por UF no último ano ----------
    total_uf = (
        silver.filter(F.col("ano") == ano_max)
        .groupBy("uf")
        .agg(F.sum("acessos").alias("total_acessos"))
        .orderBy(F.desc("total_acessos"))
    )
    gold4_out = os.path.join(lake_root, "gold", "q2_total_acessos_por_uf", f"ano={ano_max}")
    ensure_dir(gold4_out)
    (
        total_uf.write.mode("overwrite")
        .format(output_format)
        .save(gold4_out)
    )

    # ---------- GOLD5: evolução por UF + TECNOLOGIA (últimos 3 anos) ----------
    evol_uf_tec = (
        silver.filter(F.col("ano").isin(anos))  # 'anos' já é [ano_max-2, ano_max-1, ano_max]
        .groupBy("ano", "uf", "tecnologia")
        .agg(F.sum("acessos").alias("total_acessos"))
        .orderBy("ano", "uf", "tecnologia")
    )
    gold5_out = os.path.join(
        lake_root, "gold", "q2_evolucao_por_uf_tecnologia", f"anos={anos[0]}_{anos[-1]}"
    )
    ensure_dir(gold5_out)
    for y in anos:
        (
            evol_uf_tec.filter(F.col("ano") == y)
            .repartition(1)
            .write.mode("overwrite")
            .format(output_format)
            .option("maxRecordsPerFile", "1000000")
            .save(os.path.join(gold5_out, f"ano={y}"))
    )

    spark.stop()

    manifest = {
        "silver_path": silver_out,
        "gold_total_regiao_path": gold1_out,
        "gold_evol_tec_path": gold2_out,
        "gold_evol_regiao_tec_path": gold3_out,
        "ano_max": int(ano_max),
        "anos_considerados": [int(x) for x in anos],
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_bronze": bronze_dir,
        "output_format": output_format,
    }

    # ---------- (4) STATS JSON PARA AUDITORIA ----------
    stats = {
        "entrada": {
            "csv_encontrados": len(csv_files),
            "dataframes_validos": len(dfs),
            "arquivos_ignorados": [name for name, _ in skipped],
        },
        "silver": {
            "linhas": int(total_rows),
            "anos_unicos": [int(a) for a in anos_uniqs],
            "linhas_por_ano": {int(k): int(v) for k, v in linhas_por_ano.items()},
            "min_ano": int(minmax.get("min_ano")) if minmax.get("min_ano") is not None else None,
            "max_ano": int(minmax.get("max_ano")) if minmax.get("max_ano") is not None else None,
        },
        "gold": {
            "total_por_regiao_path": gold1_out,
            "evolucao_por_tec_path": gold2_out,
            "evolucao_por_regiao_tec_path": gold3_out,
        },
    }
    write_json(os.path.join(lake_root, "gold", "q2_run_stats.json"), stats)
    write_json(os.path.join(lake_root, "gold", "q2_manifest.json"), manifest)


def main():
    p = argparse.ArgumentParser(description="Q2 - Transformações Spark para silver/gold")
    p.add_argument("--datalake", default=None, help="Raiz do Data Lake (default: ./data_lake ou env DATA_LAKE_ROOT)")
    p.add_argument("--format", default="parquet", choices=["parquet"], help="Formato de saída")
    p.add_argument(
        "--single-file",
        action="store_true",
        help="(opcional) grava cada dataset em um único arquivo (coalesce(1)); útil localmente",
    )
    p.add_argument(
        "--gold-extra",
        action="store_true",
        help="(opcional) gera gold extra: evolução por região + tecnologia (últimos 3 anos)",
    )
    args = p.parse_args()
    transform_q2(args.datalake, args.format, single_file=args.single_file, gold_extra=args.gold_extra)


if __name__ == "__main__":
    main()
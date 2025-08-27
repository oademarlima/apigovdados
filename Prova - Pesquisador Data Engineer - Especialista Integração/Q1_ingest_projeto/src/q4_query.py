# -*- coding: utf-8 -*-
"""
Q4 – responder perguntas sobre os dados do GOLD via n8n (ou terminal).

Entrada:
- Lê o corpo do webhook a partir da env var N8N_WEBHOOK_BODY (JSON).
  Ex.: {"question":"total por região no ano 2025"}

Saídas:
- Imprime em stdout um JSON com "answer" (string) e/ou "error" e "details".
"""

import os
import re
import json
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa


# ---------------------------- util ----------------------------

def env_json(name: str, default=None):
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def json_out(obj):
    print(json.dumps(obj, ensure_ascii=False))


def fmt(x):
    """Formata inteiros com separador de milhar (ponto)."""
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return str(x)


def normalize_cols(table):
    """Cria um dict {nome_padrao: nome_coluna} tolerante a variações."""
    names = {n.lower(): n for n in table.schema.names}

    def pick(*cands):
        for c in cands:
            key = c.lower()
            if key in names:
                return names[key]
        return None

    return {
        "regiao": pick("regiao", "região", "region"),
        "ano": pick("ano", "year"),
        "tecnologia": pick("tecnologia", "technology", "tecnologia_acesso"),
        "total": pick("total_acessos", "total", "qtd", "valor"),
    }


# ---------------------------- loaders ----------------------------

def load_total_por_regiao(gold_dir: Path, ano: int):
    base = gold_dir / "q2_total_acessos_por_regiao" / f"ano={ano}"
    if not base.exists():
        return None
    dataset = ds.dataset(str(base), format="parquet")
    return dataset.to_table()


def available_years_total_por_regiao(gold_dir: Path):
    base = gold_dir / "q2_total_acessos_por_regiao"
    if not base.exists():
        return []
    anos = []
    for d in base.iterdir():
        if d.is_dir() and d.name.startswith("ano="):
            try:
                anos.append(int(d.name.split("=")[1]))
            except Exception:
                pass
    return sorted(set(anos))


def pick_latest_ano_available(gold_dir: Path):
    anos = available_years_total_por_regiao(gold_dir)
    return max(anos) if anos else None


def load_evolucao_por_tecnologia(gold_dir: Path):
    base = gold_dir / "q2_evolucao_por_tecnologia"
    if not base.exists():
        return None, None
    dirs = sorted([d.name for d in base.iterdir() if d.is_dir() and d.name.startswith("anos=")])
    if not dirs:
        return None, None
    intervalo = dirs[-1]  # ex.: anos=2023_2025
    ds_path = base / intervalo
    dataset = ds.dataset(str(ds_path), format="parquet", partitioning="hive")
    table = dataset.to_table()
    return table, intervalo


def load_evolucao_por_regiao_tecnologia(gold_dir: Path):
    """
    Lê gold/q2_evolucao_por_regiao_tecnologia/anos=XXXX_YYYY/
    Retorna (table, intervalo_str) ou (None, None) se não existir.
    """
    base = gold_dir / "q2_evolucao_por_regiao_tecnologia"
    if not base.exists():
        return None, None
    dirs = sorted([d.name for d in base.iterdir() if d.is_dir() and d.name.startswith("anos=")])
    if not dirs:
        return None, None
    intervalo = dirs[-1]  # mais recente
    ds_path = base / intervalo
    dataset = ds.dataset(str(ds_path), format="parquet", partitioning="hive")
    table = dataset.to_table()  # esperado: ano, regiao, tecnologia, total_acessos
    return table, intervalo


# ---------------------------- parsing ----------------------------

def parse_question(body: dict) -> str:
    q = ""
    if isinstance(body, dict):
        q = (body.get("question") or body.get("pergunta") or "").strip()
    return q.lower()


TEC_MAP = {
    "fibra": "Fibra",
    "ftth": "Fibra",
    "fttx": "Fibra",
    "cabo": "Cabo",
    "xdsl": "xDSL",
    "adsl": "xDSL",
    "rádio": "Rádio",
    "radio": "Rádio",
    "satélite": "Satélite",
    "satelite": "Satélite",
}

REGIOES = {
    "norte": "Norte",
    "nordeste": "Nordeste",
    "centro-oeste": "Centro-Oeste",
    "centro oeste": "Centro-Oeste",
    "sudeste": "Sudeste",
    "sul": "Sul",
}

def parse_tecnologia(q: str):
    for k, v in TEC_MAP.items():
        if k in q:
            return v
    return None

def parse_regiao(q: str):
    for k, v in REGIOES.items():
        if k in q:
            return v
    return None

def parse_ano(q: str):
    m = re.search(r"ano\s*=?\s*(\d{4})", q)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d{4})", q)
    return int(m.group(1)) if m else None

def parse_de_ate(q: str):
    # "entre 2020 e 2025", "de 2020 a 2025", "de 2020 até 2025"
    m = re.search(r"(?:entre|de)\s*(\d{4})\s*(?:a|até|ate|e)\s*(\d{4})", q)
    if m:
        a0, a1 = int(m.group(1)), int(m.group(2))
        if a0 > a1:
            a0, a1 = a1, a0
        return a0, a1
    return None, None

def parse_desde(q: str):
    m = re.search(r"desde\s*(\d{4})", q)
    if m:
        return int(m.group(1))
    return None

def parse_duas_regioes(q: str):
    """Extrai até duas regiões citadas no texto (case-insensitive)."""
    hits = []
    for k, v in REGIOES.items():
        if k in q and v not in hits:
            hits.append(v)
    return hits[:2]

def parse_anos_window(q: str, default=2):
    m = re.search(r"últim[oa]s?\s+(\d+)\s+anos", q)
    if m:
        return int(m.group(1))
    m = re.search(r"ultim[oa]s?\s+(\d+)\s+anos", q)  # sem acento
    if m:
        return int(m.group(1))
    return default


# ---------------------------- respostas (A, B, C, D) ----------------------------

# A) Totais regionais

def answer_total_por_regiao(gold_dir: Path, q: str):
    ano = parse_ano(q) or pick_latest_ano_available(gold_dir)
    if not ano:
        return {"error": "Não encontrei nenhum ano em q2_total_acessos_por_regiao."}

    table = load_total_por_regiao(gold_dir, ano)
    if table is None or table.num_rows == 0:
        return {"error": f"Não encontrei dados para ano={ano} em q2_total_acessos_por_regiao."}

    cols = normalize_cols(table)
    col_reg = cols["regiao"] or "regiao"
    col_tot = cols["total"] or "total_acessos"

    if col_tot in table.schema.names:
        idx = pc.sort_indices(table, sort_keys=[(col_tot, "descending")])
        table = table.take(idx)

    items = [
        {"regiao": r.get(col_reg), "total_acessos": r.get(col_tot)}
        for r in table.to_pylist()
        if r.get(col_reg) is not None
    ]

    top = items[:5]
    ans = "Totais por região em {ano} (top {k}): {lista}".format(
        ano=ano,
        k=min(5, len(items)),
        lista=", ".join([f"{i['regiao']}: {fmt(i['total_acessos'])}" for i in top]),
    )
    return {"answer": ans, "details": {"ano": ano, "top5": top}, "used_dataset": "gold/q2_total_acessos_por_regiao"}


def top_regiao_no_ano(gold_dir: Path, ano: int, pick: str = "max"):
    t = load_total_por_regiao(gold_dir, ano)
    if t is None or t.num_rows == 0:
        return None, None, []

    rows = [
        r for r in t.to_pylist()
        if r.get("regiao") is not None and r.get("total_acessos") is not None
    ]
    if not rows:
        return None, None, []

    rows.sort(key=lambda r: r["total_acessos"], reverse=(pick == "max"))
    best = rows[0]
    return best["regiao"], best["total_acessos"], rows[:5]


# B) Crescimento por tecnologia

def variacao_por_tecnologia_window(gold_dir: Path, window_years: int, pick: str = "max"):
    table, intervalo = load_evolucao_por_tecnologia(gold_dir)
    if table is None:
        return None, None, None, None, None, None, []

    anos = sorted(set(table.column("ano").to_pylist()))
    if len(anos) < 2:
        return None, None, None, None, None, intervalo, []

    base_ano = max(anos) - window_years
    anos_ref = [a for a in anos if a >= base_ano]
    if len(anos_ref) < 2:
        anos_ref = anos[-2:]

    by_tec = {}
    for row in table.to_pylist():
        a = row.get("ano"); tec = row.get("tecnologia"); tot = row.get("total_acessos")
        if a in anos_ref and tec is not None and tot is not None:
            by_tec.setdefault(tec, {})[a] = tot

    a0, a1 = min(anos_ref), max(anos_ref)
    stats = []
    for tec, series in by_tec.items():
        if a0 in series and a1 in series:
            first = series[a0]; last = series[a1]
            stats.append((tec, last - first, first, last))

    if not stats:
        return None, None, None, None, anos_ref, intervalo, []

    stats.sort(key=lambda x: x[1], reverse=(pick == "max"))
    tec, delta, first, last = stats[0]
    top5 = [{"tecnologia": t, "delta": d, "inicio": f, "fim": l} for (t, d, f, l) in stats[:5]]
    return tec, delta, first, last, anos_ref, intervalo, top5


def answer_crescimento_por_tecnologia(gold_dir: Path, q: str):
    window = parse_anos_window(q, default=2)
    target_tec = parse_tecnologia(q)

    table, intervalo = load_evolucao_por_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "Não encontrei q2_evolucao_por_tecnologia no gold."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    if target_tec:
        mask = pc.equal(pc.utf8_lower(table[col_tec]), pa.scalar(target_tec.lower()))
        table_f = table.filter(mask)
    else:
        table_f = table

    if table_f.num_rows == 0:
        tecs = sorted(set(table.column(col_tec).to_pylist()))
        return {"error": f"Não encontrei tecnologia '{target_tec}'. Tecnologias (amostra): {tecs[:20]}"}

    table = table_f

    anos = sorted(set([r.get(col_ano) for r in table.to_pylist() if r.get(col_ano) is not None]))
    if len(anos) < 2:
        return {"error": "Dados insuficientes para calcular crescimento."}

    base_ano = max(anos) - window
    anos_ref = [a for a in anos if a >= base_ano]
    if len(anos_ref) < 2:
        anos_ref = anos[-2:]

    by_tec = {}
    for r in table.to_pylist():
        a = r.get(col_ano); t = r.get(col_tec); v = r.get(col_tot)
        if a is None or t is None or v is None:
            continue
        if a not in anos_ref:
            continue
        by_tec.setdefault(t, {})[a] = v

    cresc = []
    for tec, series in by_tec.items():
        first = series.get(min(anos_ref))
        last = series.get(max(anos_ref))
        if first is not None and last is not None:
            cresc.append((tec, last - first, first, last))

    if not cresc:
        return {"error": "Não consegui calcular crescimento nessa janela."}

    cresc.sort(key=lambda x: x[1], reverse=True)
    tec, delta, first, last = cresc[0]
    anos_delta = max(anos_ref) - min(anos_ref)
    ans = f"Nos últimos {anos_delta} anos, **{tec}** foi a que mais cresceu: {fmt(first)} → {fmt(last)} (Δ={fmt(delta)})."
    if target_tec:
        ans = f"Variação de **{tec}** nos últimos {anos_delta} anos: {fmt(first)} → {fmt(last)} (Δ={fmt(delta)})."

    return {
        "answer": ans,
        "details": {
            "intervalo": intervalo,
            "anos_considerados": anos_ref,
            "top_crescimentos": [{"tecnologia": t, "delta": d, "inicio": f, "fim": l} for t, d, f, l in cresc[:5]],
        },
        "used_dataset": "gold/q2_evolucao_por_tecnologia",
    }


# C) Região + Tecnologia (GOLD3)

def _ci_equals(col, value: str):
    return pc.equal(pc.utf8_lower(col), pa.scalar(value.lower()))

# --- util para filtro seguro em PyArrow Table ---
def _mask_eq_numeric(table, col, val):
    arr = table[col].combine_chunks()
    # tenta normalizar o valor para inteiro
    try:
        ival = int(val)
    except Exception:
        # se não der para int, compara como está
        return pc.equal(arr, pa.scalar(val))
    # garante que a coluna é inteira antes da comparação
    if not pa.types.is_integer(arr.type):
        arr = pc.cast(arr, pa.int64())
    return pc.equal(arr, pa.scalar(ival))

def _mask_eq_text_ci(table, col, text):
    arr = table[col].combine_chunks()
    return pc.equal(pc.utf8_lower(arr), pa.scalar(str(text).lower()))

def crescimento_regiao_da_tecnologia_window(gold_dir: Path, tec: str, window: int = 2):
    table, intervalo = load_evolucao_por_regiao_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "GOLD3 não encontrado: q2_evolucao_por_regiao_tecnologia."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_reg = cols["regiao"] or "regiao"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    tt = table.filter(_ci_equals(table[col_tec], tec))
    if tt.num_rows == 0:
        return {"error": f"Não encontrei registros para tecnologia '{tec}' no GOLD3."}

    anos = sorted(set([r.get(col_ano) for r in tt.to_pylist() if r.get(col_ano) is not None]))
    if len(anos) < 2:
        return {"error": "Dados insuficientes para calcular crescimento."}
    base_ano = max(anos) - window
    anos_ref = [a for a in anos if a >= base_ano]
    if len(anos_ref) < 2:
        anos_ref = anos[-2:]
    a0, a1 = min(anos_ref), max(anos_ref)

    by_reg = {}
    for r in tt.to_pylist():
        a = r.get(col_ano); reg = r.get(col_reg); v = r.get(col_tot)
        if a in anos_ref and reg is not None and v is not None:
            by_reg.setdefault(reg, {})[a] = v

    stats = []
    for reg, series in by_reg.items():
        if a0 in series and a1 in series:
            first, last = series[a0], series[a1]
            stats.append((reg, last - first, first, last))

    if not stats:
        return {"error": "Não consegui calcular crescimento para essa janela."}

    stats.sort(key=lambda x: x[1], reverse=True)
    reg, delta, first, last = stats[0]
    ans = f"A região com maior crescimento de **{tec}** foi **{reg}**: {fmt(first)} → {fmt(last)} (Δ={fmt(delta)}) nos últimos {a1-a0} anos."
    return {
        "answer": ans,
        "details": {
            "tecnologia": tec,
            "anos_considerados": [a0, a1],
            "top5_regioes": [{"regiao": r, "delta": d, "inicio": f, "fim": l} for (r, d, f, l) in stats[:5]],
            "intervalo_dir": intervalo,
        },
        "used_dataset": "gold/q2_evolucao_por_regiao_tecnologia",
    }


def min_regiao_tec_no_ano(gold_dir: Path, tec: str, ano: int):
    table, intervalo = load_evolucao_por_regiao_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "GOLD3 não encontrado: q2_evolucao_por_regiao_tecnologia."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_reg = cols["regiao"] or "regiao"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    mask_ano = _mask_eq_numeric(table, col_ano, ano)
    mask_tec = _mask_eq_text_ci(table, col_tec, tec)
    tt = table.filter(pc.and_(mask_ano, mask_tec))
    rows = [r for r in tt.to_pylist() if r.get(col_reg) is not None and r.get(col_tot) is not None]
    if not rows:
        return {"error": f"Não encontrei {tec} em {ano} no GOLD3."}

    rows.sort(key=lambda r: r[col_tot])  # asc → menor primeiro
    reg, tot = rows[0][col_reg], rows[0][col_tot]
    ans = f"Em {ano}, a região com menor número de acessos de **{tec}** foi **{reg}** ({fmt(tot)})."
    return {
        "answer": ans,
        "details": {"ano": ano, "tecnologia": tec, "bottom5": rows[:5]},
        "used_dataset": "gold/q2_evolucao_por_regiao_tecnologia",
    }


def serie_regiao_tec_intervalo(gold_dir: Path, reg: str, tec: str, a0: int, a1: int):
    table, intervalo = load_evolucao_por_regiao_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "GOLD3 não encontrado: q2_evolucao_por_regiao_tecnologia."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_reg = cols["regiao"] or "regiao"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    tt = table.filter(pc.and_(_ci_equals(table[col_tec], tec), _ci_equals(table[col_reg], reg)))
    serie = {}
    for r in tt.to_pylist():
        a = r.get(col_ano); v = r.get(col_tot)
        if a is not None and a0 <= a <= a1 and v is not None:
            serie[a] = v

    if not serie:
        return {"error": f"Não encontrei valores de {tec} em {reg} entre {a0} e {a1}."}

    seq = [{"ano": a, "total": serie[a]} for a in sorted(serie.keys())]
    ans = f"Evolução de **{tec}** em **{reg}** ({a0}–{a1}): " + ", ".join([f"{p['ano']}: {fmt(p['total'])}" for p in seq])
    return {
        "answer": ans,
        "details": {"regiao": reg, "tecnologia": tec, "intervalo": [a0, a1], "serie": seq, "intervalo_dir": intervalo},
        "used_dataset": "gold/q2_evolucao_por_regiao_tecnologia",
    }


def top_regioes_tec_no_ano(gold_dir: Path, tec: str, ano: int, k: int = 3):
    table, intervalo = load_evolucao_por_regiao_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "GOLD3 não encontrado: q2_evolucao_por_regiao_tecnologia."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_reg = cols["regiao"] or "regiao"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    mask_ano = _mask_eq_numeric(table, col_ano, ano)
    mask_tec = _mask_eq_text_ci(table, col_tec, tec)
    tt = table.filter(pc.and_(mask_ano, mask_tec))
    rows = [r for r in tt.to_pylist() if r.get(col_reg) is not None and r.get(col_tot) is not None]
    if not rows:
        return {"error": f"Não encontrei {tec} em {ano} no GOLD3."}

    rows.sort(key=lambda r: r[col_tot], reverse=True)
    topk = [{"regiao": r[col_reg], "total_acessos": r[col_tot]} for r in rows[:k]]
    ans = f"Top {k} regiões em **{tec}** ({ano}): " + "; ".join([f"{x['regiao']} ({fmt(x['total_acessos'])})" for x in topk])
    return {
        "answer": ans,
        "details": {"ano": ano, "tecnologia": tec, "top": topk},
        "used_dataset": "gold/q2_evolucao_por_regiao_tecnologia",
    }

    
def answer_participacao_regioes_ano(gold_dir: Path, ano: int):
    """Participação (%) de cada região no total do ano."""
    t = load_total_por_regiao(gold_dir, ano)
    if t is None or t.num_rows == 0:
        return {"error": f"Não encontrei dados para {ano} em q2_total_acessos_por_regiao."}

    cols = normalize_cols(t)
    col_reg = cols["regiao"] or "regiao"
    col_tot = cols["total"] or "total_acessos"

    rows = [r for r in t.to_pylist() if r.get(col_reg) is not None and r.get(col_tot) is not None]
    if not rows:
        return {"error": f"Sem linhas válidas para {ano}."}

    total_geral = sum(r[col_tot] for r in rows)
    if total_geral == 0:
        return {"error": "Total geral zerado; não é possível calcular participação."}

    partes = []
    for r in rows:
        pct = 100.0 * (r[col_tot] / total_geral)
        partes.append({"regiao": r[col_reg], "total_acessos": r[col_tot], "participacao_pct": round(pct, 2)})

    partes.sort(key=lambda x: x["participacao_pct"], reverse=True)
    lista = ", ".join([f"{p['regiao']}: {p['participacao_pct']}%" for p in partes])
    return {
        "answer": f"Participação por região em {ano}: {lista}.",
        "details": {"ano": ano, "participacao": partes, "total_geral": total_geral},
        "used_dataset": "gold/q2_total_acessos_por_regiao",
    }


def answer_topk_regioes_ano(gold_dir: Path, ano: int, k: int = 3):
    """Top K regiões por acessos no ano (GOLD1)."""
    t = load_total_por_regiao(gold_dir, ano)
    if t is None or t.num_rows == 0:
        return {"error": f"Não encontrei dados para {ano} em q2_total_acessos_por_regiao."}

    cols = normalize_cols(t)
    col_reg = cols["regiao"] or "regiao"
    col_tot = cols["total"] or "total_acessos"

    rows = [r for r in t.to_pylist() if r.get(col_reg) is not None and r.get(col_tot) is not None]
    rows.sort(key=lambda r: r[col_tot], reverse=True)
    topk = [{"regiao": r[col_reg], "total_acessos": r[col_tot]} for r in rows[:k]]

    ans = f"Top {k} regiões por acessos em {ano}: " + "; ".join([f"{x['regiao']} ({fmt(x['total_acessos'])})" for x in topk])
    return {"answer": ans, "details": {"ano": ano, "top": topk}, "used_dataset": "gold/q2_total_acessos_por_regiao"}


def answer_comparar_duas_regioes_ano(gold_dir: Path, ano: int, reg1: str, reg2: str):
    """Compara totais de acessos entre duas regiões em um ano (GOLD1)."""
    t = load_total_por_regiao(gold_dir, ano)
    if t is None or t.num_rows == 0:
        return {"error": f"Não encontrei dados para {ano} em q2_total_acessos_por_regiao."}

    cols = normalize_cols(t)
    col_reg = cols["regiao"] or "regiao"
    col_tot = cols["total"] or "total_acessos"

    # mapeia região -> total
    d = {}
    for r in t.to_pylist():
        rr = r.get(col_reg); vv = r.get(col_tot)
        if rr is not None and vv is not None:
            d[rr] = vv

    v1 = d.get(reg1); v2 = d.get(reg2)
    if v1 is None and v2 is None:
        return {"error": f"Não encontrei {reg1} e {reg2} em {ano}."}

    def _fmt(reg, val): return f"{reg}: {fmt(val) if val is not None else 'sem dados'}"
    ans = f"Em {ano} — {_fmt(reg1, v1)} vs {_fmt(reg2, v2)}."
    if v1 is not None and v2 is not None:
        diff = v1 - v2
        comp = "mais" if diff > 0 else ("menos" if diff < 0 else "o mesmo")
        ans += f" ({reg1} tem {comp} que {reg2}: Δ={fmt(abs(diff))})"

    return {"answer": ans, "details": {"ano": ano, "regioes": {reg1: v1, reg2: v2}}, "used_dataset": "gold/q2_total_acessos_por_regiao"}


def answer_listar_tecnologias(gold_dir: Path):
    """Lista tecnologias existentes (GOLD2 e, se houver, GOLD3)."""
    tecs = set()
    # GOLD2
    t2, _ = load_evolucao_por_tecnologia(gold_dir)
    if t2 is not None and t2.num_rows > 0:
        cols2 = normalize_cols(t2); ctec2 = cols2["tecnologia"] or "tecnologia"
        tecs.update([r.get(ctec2) for r in t2.to_pylist() if r.get(ctec2)])

    # GOLD3
    t3, _ = load_evolucao_por_regiao_tecnologia(gold_dir)
    if t3 is not None and t3.num_rows > 0:
        cols3 = normalize_cols(t3); ctec3 = cols3["tecnologia"] or "tecnologia"
        tecs.update([r.get(ctec3) for r in t3.to_pylist() if r.get(ctec3)])

    if not tecs:
        return {"error": "Não encontrei tecnologias no gold."}
    lst = sorted(tecs)
    return {"answer": "Tecnologias disponíveis: " + ", ".join(lst) + ".", "details": {"tecnologias": lst}}


def answer_listar_regioes(gold_dir: Path):
    """Lista regiões existentes (preferência GOLD3; fallback GOLD1 mais recente)."""
    regs = set()

    # GOLD3 primeiro
    t3, _ = load_evolucao_por_regiao_tecnologia(gold_dir)
    if t3 is not None and t3.num_rows > 0:
        cols3 = normalize_cols(t3); creg3 = cols3["regiao"] or "regiao"
        regs.update([r.get(creg3) for r in t3.to_pylist() if r.get(creg3)])

    # Fallback GOLD1 (mais recente)
    if not regs:
        ano = pick_latest_ano_available(gold_dir)
        if ano:
            t1 = load_total_por_regiao(gold_dir, ano)
            if t1 is not None and t1.num_rows > 0:
                cols1 = normalize_cols(t1); creg1 = cols1["regiao"] or "regiao"
                regs.update([r.get(creg1) for r in t1.to_pylist() if r.get(creg1)])

    if not regs:
        return {"error": "Não encontrei regiões no gold."}
    lst = sorted(regs)
    return {"answer": "Regiões disponíveis: " + ", ".join(lst) + ".", "details": {"regioes": lst}}


def answer_listar_intervalos_gold(gold_dir: Path):
    """Lista intervalos de anos disponíveis em GOLD2 e GOLD3."""
    base2 = gold_dir / "q2_evolucao_por_tecnologia"
    base3 = gold_dir / "q2_evolucao_por_regiao_tecnologia"

    def _intervalos(base: Path):
        if not base.exists():
            return []
        ints = [d.name for d in base.iterdir() if d.is_dir() and d.name.startswith("anos=")]
        return sorted(ints)

    ints2 = _intervalos(base2)
    ints3 = _intervalos(base3)

    if not ints2 and not ints3:
        return {"error": "Não encontrei intervalos em GOLD2/GOLD3."}

    return {
        "answer": "Intervalos encontrados — GOLD2: " + (", ".join(ints2) if ints2 else "nenhum") +
                  " | GOLD3: " + (", ".join(ints3) if ints3 else "nenhum") + ".",
        "details": {"gold2_intervalos": ints2, "gold3_intervalos": ints3}
    }

# ---------------------------- EXTRAS A: Totais regionais (GOLD1) ----------------------------

def answer_variacao_total_regiao_intervalo(gold_dir: Path, reg: str, a0: int, a1: int):
    """Δ absoluto do total de acessos da REGIÃO entre A e B usando GOLD1 ano a ano."""
    if a0 > a1:
        a0, a1 = a1, a0
    serie = []
    for ano in range(a0, a1 + 1):
        t = load_total_por_regiao(gold_dir, ano)
        if t is None or t.num_rows == 0:
            continue  # <- aqui está dentro do loop, ok

        cols = normalize_cols(t)
        col_reg = cols["regiao"] or "regiao"
        col_tot = cols["total"] or "total_acessos"

        # filtro case-insensitive pela região
        tt = t.filter(_mask_eq_text_ci(t, col_reg, reg))
        if tt.num_rows == 0:
            continue  # <- idem, dentro do loop

        total = sum((r.get(col_tot) or 0) for r in tt.to_pylist())
        serie.append((ano, total))

    if len(serie) < 2:
        return {"error": f"Não foi possível montar a série de {reg} em {a0}-{a1}."}

    serie.sort(key=lambda x: x[0])
    first_y, first_v = serie[0]
    last_y, last_v = serie[-1]
    delta = (last_v or 0) - (first_v or 0)
    return {
        "answer": f"De {first_y} a {last_y}, **{reg}** variou de {fmt(first_v)} para {fmt(last_v)} (Δ={fmt(delta)}).",
        "details": {
            "regiao": reg,
            "intervalo": [first_y, last_y],
            "serie": [{"ano": a, "total": v} for a, v in serie],
        },
        "used_dataset": "gold/q2_total_acessos_por_regiao",
    }


def answer_regiao_mais_cresceu_total_intervalo(gold_dir: Path, a0: int, a1: int):
    """Entre A e B, qual região mais cresceu em acessos totais? (GOLD1 somando por ano)."""
    if a0 > a1:
        a0, a1 = a1, a0
    # constrói mapa: regiao -> {ano: total}
    by_reg = {}
    for ano in range(a0, a1 + 1):
        t = load_total_por_regiao(gold_dir, ano)
        if t is None or t.num_rows == 0:
            continue
        cols = normalize_cols(t)
        col_reg = cols["regiao"] or "regiao"
        col_tot = cols["total"] or "total_acessos"
        for r in t.to_pylist():
            reg = r.get(col_reg); v = r.get(col_tot)
            if reg is None or v is None:
                continue
            by_reg.setdefault(reg, {})[ano] = (by_reg.get(reg, {}).get(ano, 0) + v)
    if not by_reg:
        return {"error": f"Sem dados para {a0}-{a1}."}
    a_first, a_last = a0, a1
    stats = []
    for reg, series in by_reg.items():
        if a_first in series and a_last in series:
            first, last = series[a_first], series[a_last]
            stats.append((reg, last - first, first, last))
    if not stats:
        return {"error": f"Séries incompletas para {a0}-{a1}."}
    stats.sort(key=lambda x: x[1], reverse=True)
    reg, delta, first, last = stats[0]
    return {
        "answer": f"Entre {a0} e {a1}, quem mais cresceu foi **{reg}**: {fmt(first)} → {fmt(last)} (Δ={fmt(delta)}).",
        "details": {"intervalo": [a0, a1], "top": [{"regiao": r, "delta": d, "inicio": f, "fim": l} for (r,d,f,l) in stats[:5]]},
        "used_dataset": "gold/q2_total_acessos_por_regiao",
    }


def answer_ranking_regional_ano(gold_dir: Path, ano: int):
    """Ranking completo de regiões por total em um ano (GOLD1)."""
    t = load_total_por_regiao(gold_dir, ano)
    if t is None or t.num_rows == 0:
        return {"error": f"Não encontrei dados para ano={ano}."}
    cols = normalize_cols(t)
    col_reg = cols["regiao"] or "regiao"
    col_tot = cols["total"] or "total_acessos"
    rows = [r for r in t.to_pylist() if r.get(col_reg) is not None and r.get(col_tot) is not None]
    if not rows:
        return {"error": f"Sem registros válidos em {ano}."}
    rows.sort(key=lambda r: r[col_tot], reverse=True)
    rank = [{"pos": i+1, "regiao": r[col_reg], "total_acessos": r[col_tot]} for i, r in enumerate(rows)]
    ans = f"Ranking regional {ano}: " + "; ".join([f"{x['pos']}º {x['regiao']} ({fmt(x['total_acessos'])})" for x in rank])
    return {"answer": ans, "details": {"ano": ano, "ranking": rank}, "used_dataset": "gold/q2_total_acessos_por_regiao"}


# ---------------------------- EXTRAS B: Crescimento por tecnologia (GOLD2) ----------------------------

def answer_tecnologia_lider_ano(gold_dir: Path, ano: int):
    table, intervalo = load_evolucao_por_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "Não encontrei q2_evolucao_por_tecnologia no gold."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    # use máscara robusta (BooleanArray), não Expression
    mask = _mask_eq_numeric(table, col_ano, ano)
    tt = table.filter(mask)

    rows = [r for r in tt.to_pylist() if r.get(col_tec) is not None and r.get(col_tot) is not None]
    if not rows:
        return {"error": f"Não encontrei tecnologias em {ano}."}

    rows.sort(key=lambda r: r[col_tot], reverse=True)
    best = rows[0]
    tec, tot = best[col_tec], best[col_tot]
    ans = f"Em {ano}, a tecnologia líder foi **{tec}** com {fmt(tot)} acessos."
    return {
        "answer": ans,
        "details": {"ano": ano, "ranking_top5": rows[:5]},
        "used_dataset": "gold/q2_evolucao_por_tecnologia",
    }


def answer_participacao_tecnologias_ano(gold_dir: Path, ano: int):
    table, intervalo = load_evolucao_por_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "Não encontrei q2_evolucao_por_tecnologia no gold."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    mask = _mask_eq_numeric(table, col_ano, ano)
    tt = table.filter(mask)
    rows = [r for r in tt.to_pylist() if r.get(col_tec) is not None and r.get(col_tot) is not None]
    if not rows:
        return {"error": f"Não encontrei tecnologias em {ano}."}

    total_geral = sum(r[col_tot] or 0 for r in rows)
    if total_geral <= 0:
        return {"error": f"Total geral de {ano} é zero."}

    parts = []
    for r in rows:
        pct = round((r[col_tot] / total_geral) * 100, 2)
        parts.append({"tecnologia": r[col_tec], "total_acessos": r[col_tot], "participacao_pct": pct})

    parts.sort(key=lambda x: x["participacao_pct"], reverse=True)
    ans = "Participação por tecnologia em {ano}: {lista}".format(
        ano=ano,
        lista=", ".join([f"{p['tecnologia']}: {p['participacao_pct']}%" for p in parts[:5]])
    )
    return {
        "answer": ans,
        "details": {"ano": ano, "participacao": parts, "total_geral": total_geral, "intervalo": intervalo},
        "used_dataset": "gold/q2_evolucao_por_tecnologia",
    }

# ---------------------------- EXTRAS C: Região + tecnologia (GOLD3) ----------------------------

def answer_regiao_lider_tecnologia_ano(gold_dir: Path, tec: str, ano: int):
    """Região líder em uma tecnologia num ano (GOLD3)."""
    table, intervalo = load_evolucao_por_regiao_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "GOLD3 não encontrado."}
    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_reg = cols["regiao"] or "regiao"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"
    mask_ano = _mask_eq_numeric(table, col_ano, ano)
    mask_tec = _mask_eq_text_ci(table, col_tec, tec)
    tt = table.filter(pc.and_(mask_ano, mask_tec))
    rows = [r for r in tt.to_pylist() if r.get(col_reg) is not None and r.get(col_tot) is not None]
    if not rows:
        return {"error": f"Sem {tec} em {ano}."}
    rows.sort(key=lambda r: r[col_tot], reverse=True)
    top = rows[0]
    return {"answer": f"Em {ano}, a região líder em **{tec}** foi **{top[col_reg]}** ({fmt(top[col_tot])}).",
            "details": {"ano": ano, "tecnologia": tec, "top5": [{"regiao": r[col_reg], "total_acessos": r[col_tot]} for r in rows[:5]]},
            "used_dataset": "gold/q2_evolucao_por_regiao_tecnologia"}


def answer_participacao_regioes_na_tecnologia_ano(gold_dir: Path, tec: str, ano: int):
    """Participação (%) das regiões dentro de uma tecnologia em um ano (GOLD3)."""
    table, intervalo = load_evolucao_por_regiao_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "GOLD3 não encontrado."}
    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_reg = cols["regiao"] or "regiao"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"
    mask_ano = _mask_eq_numeric(table, col_ano, ano)
    mask_tec = _mask_eq_text_ci(table, col_tec, tec)
    tt = table.filter(pc.and_(mask_ano, mask_tec))
    rows = [r for r in tt.to_pylist() if r.get(col_reg) is not None and r.get(col_tot) is not None]
    if not rows:
        return {"error": f"Sem {tec} em {ano}."}
    total = sum(r[col_tot] or 0 for r in rows)
    parts = [{"regiao": r[col_reg], "total": r[col_tot], "share": (r[col_tot] / total * 100 if total else 0.0)} for r in rows]
    parts.sort(key=lambda x: x["total"], reverse=True)
    ans = f"Participação regional em **{tec}** ({ano}): " + "; ".join([f"{p['regiao']} {p['share']:.1f}%" for p in parts])
    return {"answer": ans, "details": {"ano": ano, "tecnologia": tec, "shares": parts, "total": total},
            "used_dataset": "gold/q2_evolucao_por_regiao_tecnologia"}

def comparar_tec_regioes_no_ano(gold_dir: Path, tec: str, ano: int, reg1: str, reg2: str):
    table, intervalo = load_evolucao_por_regiao_tecnologia(gold_dir)
    if table is None or table.num_rows == 0:
        return {"error": "GOLD3 não encontrado: q2_evolucao_por_regiao_tecnologia."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_reg = cols["regiao"] or "regiao"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    mask_ano = _mask_eq_numeric(table, col_ano, ano)
    mask_tec = _mask_eq_text_ci(table, col_tec, tec)
    tt = table.filter(pc.and_(mask_ano, mask_tec))
    rows = {r.get(col_reg): r.get(col_tot) for r in tt.to_pylist() if r.get(col_reg) is not None}

    v1 = rows.get(reg1)
    v2 = rows.get(reg2)
    if v1 is None and v2 is None:
        return {"error": f"Não encontrei {tec} em {ano} para {reg1} e {reg2}."}

    def _fmt_pair(reg, val):
        return f"{reg}: {fmt(val) if val is not None else 'sem dados'}"

    ans = f"Em {ano}, **{tec}** — {_fmt_pair(reg1, v1)} vs {_fmt_pair(reg2, v2)}."
    if v1 is not None and v2 is not None:
        diff = v1 - v2
        comp = "mais" if diff > 0 else ("menos" if diff < 0 else "o mesmo")
        ans += f" ({reg1} tem {comp} que {reg2}: Δ={fmt(abs(diff))})"

    return {
        "answer": ans,
        "details": {"ano": ano, "tecnologia": tec, "regioes": {reg1: v1, reg2: v2}},
        "used_dataset": "gold/q2_evolucao_por_regiao_tecnologia",
    }


# D) Exploratórias

def answer_anos_disponiveis(gold_dir: Path):
    anos = available_years_total_por_regiao(gold_dir)
    if not anos:
        return {"error": "Não encontrei anos disponíveis em q2_total_acessos_por_regiao."}
    return {"answer": f"Anos disponíveis: {', '.join(map(str, anos))}.", "details": {"anos": anos},
            "used_dataset": "gold/q2_total_acessos_por_regiao"}

def answer_total_acessos_ano(gold_dir: Path, ano: int):
    t = load_total_por_regiao(gold_dir, ano)
    if t is None or t.num_rows == 0:
        return {"error": f"Não encontrei dados para {ano} em q2_total_acessos_por_regiao."}
    cols = normalize_cols(t)
    col_tot = cols["total"] or "total_acessos"
    total = sum([r.get(col_tot) or 0 for r in t.to_pylist()])
    return {"answer": f"Em {ano}, o total de acessos foi {fmt(total)}.", "details": {"ano": ano, "total": total},
            "used_dataset": "gold/q2_total_acessos_por_regiao"}

def answer_series_tecnologia_intervalo(gold_dir: Path, tec: str, a0: int, a1: int):
    table, intervalo = load_evolucao_por_tecnologia(gold_dir)
    if table is None:
        return {"error": "Não encontrei q2_evolucao_por_tecnologia no gold."}

    cols = normalize_cols(table)
    col_ano = cols["ano"] or "ano"
    col_tec = cols["tecnologia"] or "tecnologia"
    col_tot = cols["total"] or "total_acessos"

    mask = pc.equal(pc.utf8_lower(table[col_tec]), pa.scalar(tec.lower()))
    tt = table.filter(mask)
    if tt.num_rows == 0:
        return {"error": f"Não encontrei tecnologia '{tec}'."}

    serie = {}
    for r in tt.to_pylist():
        a = r.get(col_ano); v = r.get(col_tot)
        if a is not None and a0 <= a <= a1 and v is not None:
            serie[a] = v

    if not serie:
        return {"error": f"Não encontrei valores de {tec} entre {a0} e {a1}."}

    seq = [{"ano": a, "total": serie[a]} for a in sorted(serie.keys())]
    ans = f"Acessos de **{tec}** de {min(serie)} a {max(serie)}: " + ", ".join(
        [f"{p['ano']}: {fmt(p['total'])}" for p in seq]
    )
    return {"answer": ans, "details": {"tecnologia": tec, "intervalo": [a0, a1], "serie": seq},
            "used_dataset": "gold/q2_evolucao_por_tecnologia"}


# ---------------------------- roteamento ----------------------------

def main():
    DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "./data_lake")
    gold_dir = Path(DATA_LAKE_ROOT) / "gold"

    body = env_json("N8N_WEBHOOK_BODY", default={})
    question = parse_question(body)

    if not question:
        json_out({"error": "Envie JSON com a chave 'question'."})
        return

    # A) TOTAL POR REGIÃO (lista/ordenado)
    if ("total" in question and "regi" in question) or ("por região" in question) or ("por regiao" in question):
        json_out(answer_total_por_regiao(gold_dir, question)); return

    # A) REGIÃO COM MAIS ACESSOS EM ANO
    if ("região" in question or "regiao" in question) and ("mais" in question) and ("acesso" in question or "acessos" in question):
        ano = parse_ano(question) or (re.search(r"(\d{4})", question) and int(re.search(r"(\d{4})", question).group(1)))
        if not ano:
            json_out({"error": "Especifique o ano. Ex.: 'região com mais acessos em 2025'."}); return
        regiao, total, top5 = top_regiao_no_ano(gold_dir, ano, pick="max")
        if regiao is None:
            json_out({"error": f"Não encontrei dados para ano={ano}."}); return
        json_out({
            "answer": f"Em {ano}, a região com mais acessos foi **{regiao}**, com {fmt(total)} acessos.",
            "details": {"ano": ano, "top5": top5},
            "used_dataset": "gold/q2_total_acessos_por_regiao",
        }); return

    # A) REGIÃO COM MENOS ACESSOS EM ANO
    if ("região" in question or "regiao" in question) and ("menos" in question) and ("acesso" in question or "acessos" in question):
        ano = parse_ano(question) or (re.search(r"(\d{4})", question) and int(re.search(r"(\d{4})", question).group(1)))
        if not ano:
            json_out({"error": "Especifique o ano. Ex.: 'região com menos acessos em 2025'."}); return
        regiao, total, top5 = top_regiao_no_ano(gold_dir, ano, pick="min")
        if regiao is None:
            json_out({"error": f"Não encontrei dados para ano={ano}."}); return
        json_out({
            "answer": f"Em {ano}, a região com menos acessos foi **{regiao}**, com {fmt(total)} acessos.",
            "details": {"ano": ano, "bottom5": top5},
            "used_dataset": "gold/q2_total_acessos_por_regiao",
        }); return

    # A) EVOLUÇÃO DA REGIÃO EM INTERVALO
    if ("evolução" in question or "evolucao" in question) and ("região" in question or "regiao" in question):
        json_out(answer_evolucao_regiao_intervalo(gold_dir, question)); return

    # B) TECNOLOGIA QUE MAIS GANHOU
    if ("tecnologia" in question) and ("ganhou" in question or "maior crescimento" in question or "mais cresceu" in question):
        tec, delta, first, last, anos_ref, intervalo, top5 = variacao_por_tecnologia_window(
            gold_dir, window_years=parse_anos_window(question, 2), pick="max")
        if tec is None:
            json_out({"error": "Dados insuficientes para calcular crescimento."}); return
        anos_delta = max(anos_ref) - min(anos_ref)
        json_out({
            "answer": f"A tecnologia que mais ganhou acessos nos últimos {anos_delta} anos foi **{tec}**: {fmt(first)} → {fmt(last)} (Δ={fmt(delta)}).",
            "details": {"intervalo": intervalo, "anos_considerados": anos_ref, "top5": top5},
            "used_dataset": "gold/q2_evolucao_por_tecnologia",
        }); return

    # --- EXTRAS A ---
    # #1 Δ de uma REGIÃO específica entre A e B (exige intervalo explícito) — sem colisão com C1
    a0_tmp, a1_tmp = parse_de_ate(question)
    if (
        any(w in question for w in ["variação", "variacao", "delta", "crescimento"])
        and ("regi" in question)
        and a0_tmp and a1_tmp
        and (parse_tecnologia(question) is None)
    ):
        reg = parse_regiao(question)
        if not reg:
            json_out({"error": "Informe a região. Ex.: 'qual foi a variação do Sudeste entre 2020 e 2025?'"})
            return
        out = answer_variacao_total_regiao_intervalo(gold_dir, reg, a0_tmp, a1_tmp)
        json_out(out); return

    # #2 Qual REGIÃO mais cresceu em acessos totais entre A e B (exige intervalo) — sem colisão com C1
    a0_tmp, a1_tmp = parse_de_ate(question)
    if (
        ("regi" in question)
        and any(w in question for w in ["mais cresceu", "maior crescimento"])
        and a0_tmp and a1_tmp
        and (parse_tecnologia(question) is None)
    ):
        out = answer_regiao_mais_cresceu_total_intervalo(gold_dir, a0_tmp, a1_tmp)
        json_out(out); return

    # ranking regional completo em um ano
    if ("ranking" in question) and ("regi" in question) and re.search(r"\b(19|20)\d{2}\b", question):
        ano = parse_ano(question)
        if not ano:
            json_out({"error": "Informe o ano. Ex.: 'ranking regional 2025'."}); return
        out = answer_ranking_regional_ano(gold_dir, ano)
        json_out(out); return

    # --- EXTRAS B ---
    # tecnologia líder no ano
    if ("tecnolog" in question) and ("líder" in question or "lider" in question) and re.search(r"\b(19|20)\d{2}\b", question):
        ano = parse_ano(question)
        out = answer_tecnologia_lider_ano(gold_dir, ano)
        json_out(out); return

    # participação por tecnologia no ano
    if any(w in question for w in ["participação por tecnologia", "participacao por tecnologia"]) and re.search(r"\b(19|20)\d{2}\b", question):
        ano = parse_ano(question)
        out = answer_participacao_tecnologias_ano(gold_dir, ano)
        json_out(out); return

    # --- EXTRAS C ---
    # região líder em <TEC> no ano
    if ("regi" in question) and ("líder" in question or "lider" in question) and parse_tecnologia(question) and re.search(r"\b(19|20)\d{2}\b", question):
        tec = parse_tecnologia(question); ano = parse_ano(question)
        out = answer_regiao_lider_tecnologia_ano(gold_dir, tec, ano)
        json_out(out); return

    # participação das regiões dentro de <TEC> no ano
    if any(w in question for w in ["participação das regiões", "participacao das regioes", "participação regional"]) and parse_tecnologia(question) and re.search(r"\b(19|20)\d{2}\b", question):
        tec = parse_tecnologia(question); ano = parse_ano(question)
        out = answer_participacao_regioes_na_tecnologia_ano(gold_dir, tec, ano)
        json_out(out); return

    # B) TECNOLOGIA QUE MAIS PERDEU
    if ("tecnologia" in question) and ("perdeu" in question or "queda" in question or "menos cresceu" in question):
        tec, delta, first, last, anos_ref, intervalo, top5 = variacao_por_tecnologia_window(
            gold_dir, window_years=parse_anos_window(question, 2), pick="min")
        if tec is None:
            json_out({"error": "Dados insuficientes para calcular variação."}); return
        anos_delta = max(anos_ref) - min(anos_ref)
        json_out({
            "answer": f"A tecnologia que mais perdeu acessos nos últimos {anos_delta} anos foi **{tec}**: {fmt(first)} → {fmt(last)} (Δ={fmt(delta)}).",
            "details": {"intervalo": intervalo, "anos_considerados": anos_ref, "bottom5": top5},
            "used_dataset": "gold/q2_evolucao_por_tecnologia",
        }); return

    # B) TOP 3 TEC DESDE ANO
    if "top 3" in question and "tecnolog" in question and ("desde" in question):
        since = parse_desde(question)
        if not since:
            json_out({"error": "Informe o ano base. Ex.: 'top 3 tecnologias com maior crescimento desde 2020'."}); return
        json_out(answer_top3_tec_desde(gold_dir, since)); return

    # B) TEC X CRESCEU OU CAIU?
    if (("cresceu" in question) or ("caiu" in question)) and ("tecnolog" in question or parse_tecnologia(question)):
        tec = parse_tecnologia(question)
        if not tec:
            json_out({"error": "Informe a tecnologia (ex.: fibra, xDSL, cabo, rádio, satélite)."}); return
        n = parse_anos_window(question, default=5)
        json_out(answer_trend_tecnologia_ultimos(gold_dir, tec, n)); return

    # C) REGIÃO + TECNOLOGIA (GOLD3)
    # C1) Região com MAIOR crescimento de <TEC> nos últimos N anos
    if ("regi" in question) and ("crescimento" in question) and (parse_tecnologia(question)):
        tec = parse_tecnologia(question)
        n = parse_anos_window(question, default=2)
        json_out(crescimento_regiao_da_tecnologia_window(gold_dir, tec, n)); return

    # C2) Região com MENOR número de acessos de <TEC> em <ANO>
    if ("regi" in question) and ("menor" in question or "menos" in question) and ("acesso" in question) and parse_tecnologia(question) and parse_ano(question):
        tec = parse_tecnologia(question)
        ano = parse_ano(question)
        json_out(min_regiao_tec_no_ano(gold_dir, tec, ano)); return

    # C3) Série: evolução de <TEC> no <REG> entre A e B
    if ("evolu" in question) and parse_tecnologia(question) and parse_regiao(question) and any(w in question for w in ["entre", "de "]):
        tec = parse_tecnologia(question)
        reg = parse_regiao(question)
        a0, a1 = parse_de_ate(question)
        if not a0 or not a1:
            json_out({"error": "Informe o intervalo. Ex.: 'evolução de fibra no Nordeste entre 2023 e 2025'."}); return
        json_out(serie_regiao_tec_intervalo(gold_dir, reg, tec, a0, a1)); return

    # C4) Top 3 regiões em <TEC> em <ANO>
    if ("top 3" in question) and ("regi" in question) and parse_tecnologia(question) and parse_ano(question):
        tec = parse_tecnologia(question)
        ano = parse_ano(question)
        json_out(top_regioes_tec_no_ano(gold_dir, tec, ano, k=3)); return

    # C5) Comparar <TEC> no <REG1> e <REG2> em <ANO>
    def _parse_duas_regioes(q: str):
        hits = []
        for k, v in REGIOES.items():
            if k in q and v not in hits:
                hits.append(v)
        return hits[:2]

    if ("comparar" in question) and ("regi" in question) and parse_tecnologia(question) and parse_ano(question):
        tec = parse_tecnologia(question)
        ano = parse_ano(question)
        regs = _parse_duas_regioes(question)
        if len(regs) < 2:
            json_out({"error": "Informe duas regiões. Ex.: 'comparar fibra no sudeste e sul em 2024'."}); return
        json_out(comparar_tec_regioes_no_ano(gold_dir, tec, ano, regs[0], regs[1])); return

    # A) PARTICIPAÇÃO POR REGIÃO NO ANO
    if ("participa" in question) and ("regi" in question) and re.search(r"\b(19|20)\d{2}\b", question):
        ano = parse_ano(question)
        if not ano:
            json_out({"error": "Informe o ano. Ex.: 'participação de cada região em 2025'."}); return
        out = answer_participacao_regioes_ano(gold_dir, ano)
        json_out(out); return

    # A) TOP 3 REGIÕES POR ACESSOS NO ANO (sem mencionar tecnologia)
    if ("top 3" in question) and ("regi" in question) and not ("tecnolog" in question) and re.search(r"\b(19|20)\d{2}\b", question):
        ano = parse_ano(question)
        out = answer_topk_regioes_ano(gold_dir, ano, k=3)
        json_out(out); return

    # A) COMPARAR DUAS REGIÕES NO ANO (sem tecnologia)
    if ("comparar" in question) and ("regi" in question) and not ("tecnolog" in question) and re.search(r"\b(19|20)\d{2}\b", question):
        ano = parse_ano(question)
        regs = parse_duas_regioes(question)
        if len(regs) < 2:
            json_out({"error": "Informe duas regiões. Ex.: 'comparar Sudeste e Nordeste em 2025'."}); return
        out = answer_comparar_duas_regioes_ano(gold_dir, ano, regs[0], regs[1])
        json_out(out); return

    # D) LISTAR TECNOLOGIAS DISPONÍVEIS
    if ("quais" in question) and ("tecnolog" in question):
        out = answer_listar_tecnologias(gold_dir)
        json_out(out); return

    # D) LISTAR REGIÕES DISPONÍVEIS
    if ("quais" in question) and ("regi" in question):
        out = answer_listar_regioes(gold_dir)
        json_out(out); return

    # D) LISTAR INTERVALOS GOLD2/GOLD3
    if ("intervalos" in question) and ("gold" in question):
        out = answer_listar_intervalos_gold(gold_dir)
        json_out(out); return

    # D) QUAIS ANOS TÊM DADOS?
    if "quais anos" in question or "anos disponíveis" in question or "anos disponiveis" in question:
        json_out(answer_anos_disponiveis(gold_dir)); return

    # D) TOTAL DE ACESSOS EM UM ANO
    if ("quantos acessos" in question or "total de acessos" in question) and re.search(r"\b(19|20)\d{2}\b", question):
        ano = parse_ano(question)
        if not ano:
            json_out({"error": "Informe o ano. Ex.: 'Quantos acessos totais havia em 2007?'."}); return
        json_out(answer_total_acessos_ano(gold_dir, ano)); return

    # D) SÉRIE DE UMA TECNOLOGIA EM INTERVALO
    if ("mostre" in question or "mostrar" in question or "serie" in question or "série" in question) and \
       ("fibra" in question or "xdsl" in question or "cabo" in question or "rádio" in question or "radio" in question or "satélite" in question or "satelite" in question):
        tec = parse_tecnologia(question) or "Fibra"
        a0, a1 = parse_de_ate(question)
        if not a0 or not a1:
            json_out({"error": "Informe o intervalo. Ex.: 'Mostre os acessos de fibra de 2023 a 2025'."}); return
        json_out(answer_series_tecnologia_intervalo(gold_dir, tec, a0, a1)); return

    # intenção B genérica: “maior crescimento ... tecnologia ...”
    if "crescimento" in question and ("tecnologia" in question or parse_tecnologia(question)):
        json_out(answer_crescimento_por_tecnologia(gold_dir, question)); return

    # fallback
    json_out({
        "error": "Não entendi a pergunta.",
        "examples": [
            "qual região teve mais acessos em 2025?",
            "qual foi a região com menos acessos em 2025?",
            "qual foi a evolução do total de acessos da região Nordeste entre 2020 e 2025?",
            "qual tecnologia perdeu mais acessos nos últimos 3 anos?",
            "quais foram as top 3 tecnologias com maior crescimento desde 2020?",
            "a tecnologia xDSL cresceu ou caiu nos últimos 5 anos?",
            "quais anos têm dados disponíveis?",
            "quantos acessos totais havia em 2007?",
            "mostre os acessos de fibra de 2023 a 2025."
        ]
    })


if __name__ == "__main__":
    main()
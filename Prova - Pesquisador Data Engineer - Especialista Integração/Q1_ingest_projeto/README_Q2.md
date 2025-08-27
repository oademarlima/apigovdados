# Q2 – Transformações (Spark) → Silver & Gold

Este documento explica como executar a Q2, validar os resultados e exportar os *gold tables* para CSV.

---

## 1) Pré-requisitos

- Python (no venv do projeto)
- Java 17 instalado e `JAVA_HOME` configurado  
- Pacotes no venv: `pyspark`, `pyarrow`
- Q1 executada previamente (existência do *bronze* com CSV/ZIP)

> 💡 Ajuste de memória recomendado (evita erros `OutOfMemoryError`):
```bash
export SPARK_DRIVER_MEMORY=8g
export _JAVA_OPTIONS="-Xms1g -Xmx8g"
```

---

## 2) Estrutura esperada de pastas

```
Q1_ingest_projeto/
  ├── src/
  │   └── transform_q2.py
  ├── resources/
  │   ├── dim_uf_regiao.csv
  │   └── tech_map.json
  └── data_lake/
      └── bronze/
          └── dt_coleta=YYYY-MM-DD/hora=HH-MM-SS/
               ├── raw.zip   (opcional)
               └── extracted/ (opcional, CSVs extraídos)
```

Saídas criadas pela Q2:
```
data_lake/
  ├── silver/acessos_blf/
  └── gold/
       ├── q2_total_acessos_por_regiao/
       ├── q2_evolucao_por_tecnologia/
       └── q2_evolucao_por_regiao_tecnologia/   (se usar --gold-extra)
```

---

## 3) Executar a Q2

Dentro da raiz do projeto (`Q1_ingest_projeto`), com o venv ativado:

### Modo produção (particionado por ano)
```bash
python -m src.transform_q2 --datalake ./data_lake
```

### Modo local/dev (gera 1 arquivo por saída)
```bash
python -m src.transform_q2 --datalake ./data_lake --single-file
```

### Incluir gold extra (região + tecnologia)
```bash
python -m src.transform_q2 --datalake ./data_lake --gold-extra
```

---

## 4) Validação (visualização rápida no terminal)

Rode o bloco abaixo **direto no terminal**. Ele imprime *schema* e amostras do Silver e dos Golds:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

silver = spark.read.parquet("data_lake/silver/acessos_blf")
print("Silver schema:")
silver.printSchema()
print("Amostra (silver):")
silver.select("ano","mes","uf","regiao","tecnologia","acessos").show(10, truncate=False)

g1 = spark.read.parquet("data_lake/gold/q2_total_acessos_por_regiao/*/*")
print("Gold1 - total por região:")
g1.show()

g2 = spark.read.parquet("data_lake/gold/q2_evolucao_por_tecnologia/*/*")
print("Gold2 - evolução por tecnologia:")
g2.orderBy("ano","tecnologia").show(20, truncate=False)

# se rodou com --gold-extra
try:
    g3 = spark.read.parquet("data_lake/gold/q2_evolucao_por_regiao_tecnologia/*/*")
    print("Gold3 - evolução por região + tecnologia:")
    g3.orderBy("ano","regiao","tecnologia").show(20, truncate=False)
except Exception:
    print("Gold extra não encontrado (use --gold-extra para gerar).")
PY
```

---

## 5) Exportação para CSV (abrir no Excel etc.)

Gera versões CSV dos Golds em `data_lake/gold_csv/`:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.read.parquet("data_lake/gold/q2_total_acessos_por_regiao/*/*") \
     .coalesce(1).write.mode("overwrite").option("header", True) \
     .csv("data_lake/gold_csv/q2_total_por_regiao")

spark.read.parquet("data_lake/gold/q2_evolucao_por_tecnologia/*/*") \
     .coalesce(1).write.mode("overwrite").option("header", True) \
     .csv("data_lake/gold_csv/q2_evolucao_por_tecnologia")

# opcional: gold extra
try:
    spark.read.parquet("data_lake/gold/q2_evolucao_por_regiao_tecnologia/*/*") \
         .coalesce(1).write.mode("overwrite").option("header", True) \
         .csv("data_lake/gold_csv/q2_evolucao_por_regiao_tecnologia")
except Exception:
    pass

print("CSV gerados em data_lake/gold_csv/")
PY
```

Saída esperada:
```
data_lake/gold_csv/
  ├── q2_total_por_regiao/
  │     └── part-00000-....csv
  ├── q2_evolucao_por_tecnologia/
  │     └── part-00000-....csv
  └── q2_evolucao_por_regiao_tecnologia/   (se --gold-extra)
        └── part-00000-....csv
```

---

## 6) Limpar e reprocessar

Para apagar saídas e rodar novamente:

```bash
rm -rf ./data_lake/silver ./data_lake/gold ./data_lake/gold_csv
python -m src.transform_q2 --datalake ./data_lake
```

---

## 7) Solução de problemas (FAQ)

- **Erro: “Bronze não encontrado / Nenhum dt_coleta / Nenhuma pasta hora=”**  
  → Execute a Q1 primeiro e garanta que existe `data_lake/bronze/dt_coleta=.../hora=...` com CSV/ZIP.

- **Erro: PATH_NOT_FOUND para `extracted/raw.csv`**  
  → A Q2 busca CSVs em `extracted/` **ou** dentro de `raw.zip` **ou** um `raw.csv` solto. Verifique onde está o arquivo real.

- **Mensagem: “Nenhum arquivo pôde ser padronizado em (ano, mes, acessos)”**  
  → Os CSVs estão em formato “wide” (colunas `YYYY-MM`). A Q2 já tenta *melt*, mas se o arquivo for muito fora do padrão, ajuste os cabeçalhos ou confirme separador/encoding.

- **Erro: AMBIGUOUS_REFERENCE `uf` ao *join***  
  → Já tratamos no código usando `join(dim, F.upper(F.col("uf")) == dim.uf, "left")` e preservando apenas uma `uf`. Se alterou o script, garanta que o *drop* correto está aplicado.

- **Java não instalado / `JAVA_GATEWAY_EXITED`**  
  → Instale Java 17 e configure `JAVA_HOME`. No macOS via Homebrew:  
  ```bash
  brew install openjdk@17
  echo 'export JAVA_HOME=$( /usr/libexec/java_home -v 17 )' >> ~/.zshrc
  echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
  source ~/.zshrc
  ```

- **Aviso: WARN parquet.enable.dictionary**  
  → Já corrigido no script com `.config("spark.hadoop.parquet.enable.dictionary","true")`.  

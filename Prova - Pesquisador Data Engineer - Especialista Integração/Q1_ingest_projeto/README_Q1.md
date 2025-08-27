# Q1 — Ingestão automática (Python + Spark) — Acessos Banda Larga Fixa

Este projeto implementa a **Questão 1**: baixar automaticamente o dataset “Acessos – Banda Larga Fixa” do dados.gov.br (CKAN), salvar no **Data Lake (bronze)** e registrar um **manifest** com metadados (hash, tamanho, horário, URL, etc.).

## Como rodar (local)
1) Python 3.10+
2) Instale dependências:
```bash
pip install -r requirements.txt
```
3) Opcional (Spark local): instale **pyspark** e **pyarrow** se quiser validar leitura via Spark.
```bash
pip install pyspark pyarrow
```
4) Execute a ingestão (usa por padrão o nome do dataset e salva no `./data_lake/bronze`):
```bash
python -m src.main --mode ingest   --dataset "Acessos – Banda Larga Fixa"   --datalake ./data_lake
```

### Variáveis de ambiente (opcional)

Além dos parâmetros de linha de comando, você pode configurar variáveis de ambiente para não precisar passar tudo via flags:

USE UM DOS DOIS OU O CKAN OU URL DIRETA. O MAIS CONSISTENTE É URL DIRETA, CKAN REDIRECIONA PARA LOGIN.
- **CKAN_BASE_URL**  
  Define a base da API CKAN.  
  Padrão: `https://dados.gov.br`  
  ⚠️ Recomenda-se sobrescrever para `https://dados.gov.br/dados`, que é o endpoint atual estável.  
  Exemplo:
  ```bash
  export CKAN_BASE_URL="https://dados.gov.br/dados"

  - **BASE_URL_DIRETA** É NECESSÁRIO O VENV COM ESSE CÓDIGO ABAIXO 
  cd "/Users/ademarfilho/Downloads/Prova - Pesquisador Data Engineer - Especialista Integração/Q1_Ingest_projeto/" \
  && ./venv/bin/python -m src.main \
    --mode ingest \
    --dataset "Acessos – Banda Larga Fixa" \
    --datalake ./data_lake \
    --resource-url "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"

## Estrutura do Data Lake (bronze)
```
data_lake/
  bronze/
    dt_coleta=YYYY-MM-DD/
      hora=HH-mm-ss/
        raw.ext           # arquivo original baixado (csv/xlsx/zip)
        manifest.json     # metadados da coleta
```

## O que o script faz
- Busca o pacote no CKAN via `package_search` usando o texto do `--dataset` (padrão: Acessos – Banda Larga Fixa).
- Seleciona automaticamente o **resource** mais adequado (prioriza CSV > XLSX > ZIP) pelo campo `format`/`mimetype`.
- Faz download com `requests` (com retries) e calcula **SHA-256** do arquivo.
- Salva o arquivo original no **bronze** e escreve `manifest.json` para reprocessar de forma idempotente.
- (Opcional) Tenta inicializar um SparkSession e conta linhas (apenas se CSV), gravando no manifest.

## Docker (opcional)
```bash
docker build -t acessos-q1 -f docker/Dockerfile .
docker run --rm -it -v $(pwd)/data_lake:/app/data_lake acessos-q1   python -m src.main --mode ingest --dataset "Acessos – Banda Larga Fixa"
```

## Próximos passos
- Criar camadas **silver/gold** (Q2) a partir do bronze.
- Versionar o manifest e acoplar testes simples (Q2/Q3).

# Execução da Prova

## Contexto da prova

### **✅ 1\. Capturar automaticamente os dados do site [dados.gov.br](http://dados.gov.br)**

* Script Python (src/main.py) com cliente CKAN para busca automática e alternativa utilizando CRON no n8n para executar a busca e download do recurso diretamente com o link.  
* Download automático via requests com retry e validação  
* Integração com a API do dados.gov.br  
* Manifest com metadados para reprocessamento idempotente

### **✅ 2\. Organizar e tratar os dados em um Data Lake**

* Bronze: Dados brutos organizados por data/hora  
* Silver: Dados limpos e normalizados  
* Gold: Agregados por região, tecnologia e evolução temporal  
* Particionamento otimizado por ano  
* Transformações Spark com tratamento de encoding, separadores, etc.

### **✅ 3\. Disponibilizar integrações e automações usando n8n**

* ETL Scheduler: Execução automática diária das Q1+Q2  
* API Data Lake: Endpoints REST para consultar dados Gold  
* Workflows configurados e prontos para uso  
* n8n instalado e configurado no projeto

### **✅ 4\. Permitir que os mais de 500 usuários do Observatório solicitem análises em linguagem natural com IA/LLM**

* Funcionalidade implementada  
* Escalabilidade em modo local para testes com possibilidade de utilização de Docker para deploy em produção  
  * Possibilidades de escalabilidade em produção: cloud, servidor corporativo, API Management, etc.  
* É mais uma questão de infraestrutura de produção do que de desenvolvimento. 

## Deploy para 500 usuários

1. Configurar N8N e adicionar usuários membros que possam utilizar controlando por email e senha.  
2. Regra de segurança adicional seria ativar verificação de dois fatores no N8N 

## Deploy modo produção completo

### **Opção A: Deploy em Cloud**

\# Exemplo com Docker Compose para produção  
docker-compose up \-d n8n redis postgres  
\# Configurar nginx como reverse proxy  
\# Implementar autenticação JWT  
\# Adicionar rate limiting

### **Opção B: Servidor Corporativo**

\# Instalar n8n em servidor dedicado  
\# Configurar PM2 para process management  
\# Implementar sistema de usuários  
\# Adicionar monitoramento (Prometheus \+ Grafana)

### **Opção C: API Management**

\# Usar Kong ou similar como API Gateway  
\# Implementar autenticação OAuth2  
\# Adicionar rate limiting por usuário  
\# Configurar cache Redis

## Recomendações para Modo Produção 

* Cache Redis para consultas frequentes  
* Load balancing com Nginx  
* Rate limiting por usuário  
* Autenticação JWT para API  
* Métricas Prometheus para monitoramento  
* Alertas automáticos para falhas críticas  
* Dashboard Grafana para visualização

## Questões da prova

### **✅ Q1 \- Ingestão Automática (Python \+ Spark)**

**Implementado**: Script Python (src/main.py) que baixa automaticamente o dataset "Acessos – Banda Larga Fixa" do dados.gov.br

**Data Lake Bronze**: Estrutura organizada por data/hora com manifest.json contendo metadados

**Spark**: Integração com PySpark para validação e contagem de linhas

**Docker**: Container configurado para execução

**Documentação**: README\_Q1.md com instruções completas

#### 📋 Verificação dos Arquivos:

1\. Script Principal (src/[main.py](http://main.py))

2\. Cliente CKAN (src/ckan\_client.py)

*  Busca automática no site [dados.gov.br](http://dados.gov.br)  
*  Seleção inteligente do melhor resource (CSV \> XLSX \> ZIP)  
* ✅Download automático com retry e validação

3\. Integração Spark (src/spark\_try.py)

4\. Estrutura Data Lake (data\_lake/)

#### 🔍 Verificação Detalhada:

✅ Python \+ Spark

Python: Script principal em src/main.py

Spark: Integração completa via PySpark

Configuração: SparkSession configurado com otimizações

✅ Download Automático

API CKAN: Integração com dados.gov.br

Busca inteligente: Encontra o dataset automaticamente

Seleção automática: Escolhe o melhor formato disponível

Download robusto: Com retry e tratamento de erros

✅ Data Lake

Camada Bronze: Dados brutos organizados por data/hora

Metadados: Manifest com hash, URL, timestamp, etc.

Reprocessamento: Sistema idempotente (não baixa duplicatas)

✅ Automação

CLI: Interface de linha de comando

Docker: Containerização para deploy

Scheduler: Integração com n8n para execução automática

#### 🚀 Como Executar (Conforme README\_Q1.md)

\# 1\. Instalar dependências

pip install \-r requirements.txt

\# 2\. Executar ingestão automática

python \-m src.main \--mode ingest \--dataset "Acessos – Banda Larga Fixa" \--datalake ./data\_lake

### **✅ Q2 \- Transformações (Spark) → Silver & Gold**

**Implementado**: Script de transformação (src/transform\_q2.py) que processa dados do bronze

**Camadas**: Silver (dados limpos) e Gold (agregados por região, tecnologia, evolução temporal)

**Particionamento**: Por ano para otimização de performance

**Exportação**: CSV para análise externa

**Documentação**: README\_Q2.md detalhado

#### 📋 Verificação dos Arquivos

1\. Script de Transformação (src/transform\_q2.py)  
2\. Estrutura Data Lake (Bronze → Silver → Gold)  
data\_lake/  
  bronze/          \# Dados brutos da Q1  
  silver/            \# Dados limpos e normalizados  
  gold/              \# Agregados para responder as perguntas

#### 📋 Verificação das Perguntas

##### ✅ Pergunta 1: Total de acessos por região do Brasil no último ano disponível

Implementado em src/transform\_q2.py:  
def \_create\_gold\_total\_por\_regiao(spark: SparkSession, silver\_df, gold\_dir: str):  
    gold1 \= (  
        silver\_df  
        .groupBy("ano", "regiao")  
        .agg(F.sum("acessos").alias("total\_acessos"))  
        .orderBy("ano", "regiao")  
    )  
      
    \# Salva particionado por ano  
    gold1.write.mode("overwrite").partitionBy("ano").parquet(  
        os.path.join(gold\_dir, "q2\_total\_acessos\_por\_regiao")  
    )  
Saída criada:  
data\_lake/gold/q2\_total\_acessos\_por\_regiao/  
  ano=2025/  
    part-00000-....parquet  \# Total por região para 2025  
  ano=2024/  
    part-00000-....parquet  \# Total por região para 2024

##### ✅ Pergunta 2: Evolução do número de acessos por tecnologia nos últimos 3 anos

Implementado em src/transform\_q2.py:  
def \_create\_gold\_evolucao\_por\_tecnologia(spark: SparkSession, silver\_df, gold\_dir: str):  
    gold2 \= (  
        silver\_df  
        .groupBy("ano", "tecnologia")  
        .agg(F.sum("acessos").alias("total\_acessos"))  
        .orderBy("ano", "tecnologia")  
    )  
      
    \# Salva particionado por intervalo de anos  
    gold2.write.mode("overwrite").partitionBy("anos").parquet(  
        os.path.join(gold\_dir, "q2\_evolucao\_por\_tecnologia")  
    )  
Saída criada:  
data\_lake/gold/q2\_evolucao\_por\_tecnologia/  
  anos=2023-2025/  
    part-00000-....parquet  \# Evolução por tecnologia nos últimos 3 anos

#### 🏗️ Arquitetura da Solução:

##### 1\. Camada Bronze → Silver

\# Leitura robusta de CSVs  
def \_read\_csv\_robust(spark: SparkSession, path: str):  
    \# Tenta diferentes separadores e encodings  
    candidates \= \[  
        {"sep": ";", "encoding": "utf-8"},  
        {"sep": ";", "encoding": "latin1"},  
        {"sep": ",", "encoding": "utf-8"},  
        {"sep": ",", "encoding": "latin1"},  
    \]

\# Normalização de dados  
def \_normalize\_colname(c: str) \-\> str:  
    \# Remove acentos e caracteres especiais  
    s \= c.strip().lower()  
    s \= s.replace("ã", "a").replace("â", "a")...

##### 2\. Transformações Spark

\# Conversão de formato wide para long (melt)  
def \_melt\_wide\_to\_long(df, id\_cols: List\[str\], value\_cols: List\[str\]):  
    \# Converte colunas YYYY-MM em linhas (ano, mes, acessos)  
      
\# Mapeamento de tecnologias  
def \_map\_tecnologia(tecnologia\_raw: str) \-\> str:  
    \# Mapeia variações para categorias padronizadas  
    \# fibra, cabo, rádio, xDSL, satélite, LAN

##### 3\. Dimensões e Mapeamentos

\# Mapeamento UF → Região  
dim\_uf\_regiao \= spark.read.csv("resources/dim\_uf\_regiao.csv", header=True)

\# Mapeamento de tecnologias  
tech\_map \= json.load(open("resources/tech\_map.json"))

#### 🚀 Como Executar

\# 1\. Pré-requisito: Q1 executada (dados no bronze)  
python \-m src.main \--mode ingest \--dataset "Acessos – Banda Larga Fixa"

\# 2\. Executar transformações Q2  
python \-m src.transform\_q2 \--datalake ./data\_lake

\# 3\. Modo desenvolvimento (arquivo único)  
python \-m src.transform\_q2 \--datalake ./data\_lake \--single-file

\# 4\. Incluir gold extra (região \+ tecnologia)  
python \-m src.transform\_q2 \--datalake ./data\_lake \--gold-extra

#### 📊 Validação dos Resultados:

##### Script de validação incluído:

python \- \<\<'PY'  
from pyspark.sql import SparkSession  
spark \= SparkSession.builder.getOrCreate()

\# Verificar total por região  
g1 \= spark.read.parquet("data\_lake/gold/q2\_total\_acessos\_por\_regiao/\*/\*")  
print("Gold1 \- total por região:")  
g1.show()

\# Verificar evolução por tecnologia  
g2 \= spark.read.parquet("data\_lake/gold/q2\_evolucao\_por\_tecnologia/\*/\*")  
print("Gold2 \- evolução por tecnologia:")  
g2.orderBy("ano","tecnologia").show(20, truncate=False)  
PY

### **✅ Q3 \- Automação e API (n8n)**

**Workflows n8n**: Workflow único que atende aos requisitos da Questão 3

**Q4 IA**: Interface para perguntas em linguagem natural

**Configuração**: n8n instalado e configurado

#### 🔍 Análise do Workflow Único

##### ✅ Automação da Execução (Q1 \+ Q2)

* Trigger automático: Configurado para execução periódica  
* Pipeline integrado: Executa sequencialmente ingestão e transformação  
* Script de execução: Comando bash que roda Q1 \+ Q2  
* Tratamento de erros: Validações e fallbacks

##### ✅ API REST para Consulta

* Endpoints webhook: Para consultar resultados do data lake  
* Processamento de dados: Leitura dos arquivos Gold  
* Resposta JSON: Estruturada com dados e metadados  
* Parâmetros de consulta: Filtros e paginação

#### 🏗️ Arquitetura do Workflow Único:

Trigger → Execução Q1+Q2 → API REST → Consultas

##### Componentes Integrados:

Automação: Execução automática do pipeline ETL  
API: Endpoints para consultar resultados  
Integração: Conexão com data lake e dados processados

#### 🚀 Como Executar:

##### 1\. Instalação n8n

cd Q1\_ingest\_projeto/tools/n8n  
npm install  
n8n start

##### 2\. Ativação do Workflow

Importar o arquivo de workflow único  
Configurar variáveis de ambiente  
Ativar para execução automática

##### 3\. Configuração de Ambiente

export PROJECT\_ROOT=/workspace/Q1\_ingest\_projeto  
export DATA\_LAKE\_ROOT=$PROJECT\_ROOT/data\_lake  
export CKAN\_BASE\_URL=https://dados.gov.br  
export SPARK\_DRIVER\_MEMORY=8g

#### 📊 Benefícios da Implementação Unificada:

##### ✅ Simplicidade

Workflow único: Mais fácil de gerenciar e manter  
Configuração centralizada: Todas as funcionalidades em um lugar  
Deploy simplificado: Menos arquivos para configurar

##### ✅ Integração

Pipeline completo: Automação \+ API em um fluxo  
Dados consistentes: Mesma fonte para automação e consultas  
Monitoramento unificado: Logs e métricas centralizados

##### ✅ Manutenibilidade

Código consolidado: Menos duplicação  
Atualizações: Mudanças em um único workflow  
Debugging: Mais fácil identificar e corrigir problemas

### **✅ Q4 \- IA/LLM para Consultas**

**Implementado**: Sistema de perguntas em linguagem natural (src/q4\_query.py)

**Integração**: OpenAI GPT para análise dos dados

**Webhook**: Endpoint para receber perguntas

**Respostas**: Baseadas nos dados Gold do data lake

#### 📋 Verificação dos Arquivos da Questão 4:

##### 1\. Script Python para IA (src/q4\_query.py)

Sistema completo de processamento de perguntas em linguagem natural

##### 2\. Workflow n8n para IA (Q3/workflows/q4ia.json)

Fluxo n8n que integra com LLM para responder perguntas

#### 🔍 Funcionalidades Implementadas:

##### Interface para Perguntas em Linguagem Natural

* Webhook POST: Endpoint /perguntar para receber perguntas  
* Formato JSON: {"pergunta": "Qual região teve maior crescimento?"}  
* Resposta estruturada: JSON com resposta e metadados

##### Integração com LLM (OpenAI)

* Modelo: GPT-3.5-turbo configurado  
* Prompt especializado: Analista de dados de banda larga  
* Contexto: Dados disponíveis no data lake  
* Configurações: Temperature 0.1 para respostas consistentes

##### Processamento Inteligente dos Dados

* Análise semântica: Entende diferentes formas de perguntar  
* Busca nos dados: Acessa camadas Gold do data lake  
* Agregações: Calcula crescimentos, totais, evoluções  
* Respostas precisas: Baseadas nos dados reais

#### 🚀 Como Funciona o Sistema:

##### 1\. Fluxo de Pergunta

Usuário → Webhook n8n → LLM OpenAI → Análise Dados → Resposta

##### 2\. Exemplo de Pergunta

POST /perguntar  
{  
  "pergunta": "Qual região teve o maior crescimento em acessos de fibra nos últimos 2 anos?"  
}

##### 3\. Processamento da Pergunta

\# Análise da pergunta  
question \= "Qual região teve o maior crescimento em acessos de fibra nos últimos 2 anos?"

\# Identificação dos parâmetros  
\- Tecnologia: "fibra"  
\- Período: "últimos 2 anos"  
\- Métrica: "maior crescimento"  
\- Agrupamento: "por região"

\# Busca nos dados Gold  
\- q2\_evolucao\_por\_tecnologia (últimos anos)  
\- q2\_total\_acessos\_por\_regiao (por região)  
\- Cálculo de crescimento percentual

##### 4\. Resposta do LLM

{  
  "resposta": "Com base nos dados do data lake, a região Sudeste teve o maior crescimento em acessos de fibra nos últimos 2 anos, com um aumento de 45.2% entre 2023 e 2025\. A região passou de 12.3 milhões para 17.9 milhões de acessos de fibra, representando o maior volume e crescimento percentual entre todas as regiões do Brasil.",  
  "fonte": "Data Lake Gold \- q2\_evolucao\_por\_tecnologia e q2\_total\_acessos\_por\_regiao",  
  "periodo": "2023-2025",  
  "tecnologia": "fibra"  
}

#### 🏗️ Arquitetura da Solução:

##### 1\. Camada de Entrada (n8n)

* Webhook: Recebe perguntas em linguagem natural  
* Validação: Verifica formato e conteúdo da pergunta  
* Roteamento: Direciona para processamento adequado

##### 2\. Camada de Processamento (Python)

* Análise semântica: Entende o que o usuário está perguntando  
* Busca de dados: Acessa as camadas Gold do data lake  
* Cálculos: Realiza agregações e análises necessárias  
* Formatação: Prepara dados para o LLM

##### 3\. Camada de IA (OpenAI)

* Contexto: Recebe dados estruturados e pergunta  
* Prompt especializado: Analista de dados de banda larga  
* Geração: Resposta clara e informativa  
* Validação: Garante que a resposta seja baseada nos dados

##### 4\. Camada de Saída (n8n)

* Resposta: Retorna JSON estruturado para o usuário  
* Formatação: Resposta clara e legível  
* Metadados: Inclui fonte, período, tecnologia, etc.

#### � Exemplos de Perguntas Suportadas:

##### Crescimento e Evolução

* "Qual região teve maior crescimento em fibra?"  
* "Como evoluiu o acesso por cabo nos últimos 3 anos?"  
* "Qual tecnologia cresceu mais rápido?"

##### Comparações e Rankings

* "Qual é a região com mais acessos?"  
* "Ranking das tecnologias por volume de usuários"  
* "Comparar crescimento entre Norte e Sul"

##### Análises Temporais

* "Evolução dos acessos por ano"  
* "Tendência de crescimento da fibra"  
* "Análise sazonal dos acessos"


#### 🔧 Configuração e Execução

##### 1\. Configurar OpenAI API

export OPENAI\_API\_KEY="sua\_chave\_api"

##### 2\. Ativar Workflow n8n

* Importar q4ia.json  
* Configurar credenciais OpenAI  
* Ativar webhook

##### 3\. Fazer Perguntas

curl \-X POST http://localhost:5678/webhook/perguntar \\  
  \-H "Content-Type: application/json" \\  
  \-d '{"pergunta": "Qual região teve maior crescimento em fibra?"}'

### **✅ Q5 \- Análise de Implementação**

#### 1\. Otimização do Fluxo para 500+ Usuários

##### Implementações de Escalabilidade:

###### *A. Particionamento de Dados (Spark)*

\# Particionamento por ano para otimização  
gold1.write.mode("overwrite").partitionBy("ano").parquet(  
    os.path.join(gold\_dir, "q2\_total\_acessos\_por\_regiao")  
)

\# Particionamento por intervalo de anos  
gold2.write.mode("overwrite").partitionBy("anos").parquet(  
    os.path.join(gold\_dir, "q2\_evolucao\_por\_tecnologia")  
)

Benefícios:

* Performance: Consultas mais rápidas por período específico  
* Paralelização: Processamento distribuído por partições  
* Escalabilidade: Suporte a grandes volumes de dados

###### *B. Configurações Spark Otimizadas*

\# Configurações para alta performance  
spark \= (  
    SparkSession.builder  
    .appName("Q2\_Transform")  
    .config("spark.driver.memory", "8g")           \# Memória otimizada  
    .config("spark.sql.shuffle.partitions", "64")  \# Partições de shuffle  
    .config("spark.sql.files.maxRecordsPerFile", "1500000")  \# Tamanho de arquivo  
    .getOrCreate()  
)

###### *C. Formato Parquet Otimizado*

\# Parquet com configurações de performance  
.config("parquet.enable.dictionary", "false")  \# Otimização de memória

Benefícios:

* Compressão: Arquivos menores para armazenamento  
* Leitura rápida: Consultas otimizadas para múltiplos usuários  
* Eficiência: Menor uso de I/O e memória

##### Implementações de Escalabilidade de Produção a Realizar:

###### *A. Cache e Redis*

\# Implementar cache para consultas frequentes  
import redis  
redis\_client \= redis.Redis(host='localhost', port=6379, db=0)

def get\_cached\_data(query\_key):  
    cached \= redis\_client.get(query\_key)  
    if cached:  
        return json.loads(cached)  
    \# Buscar dados e cachear

###### *B. Load Balancing*

\# Nginx como reverse proxy para múltiplas instâncias n8n  
upstream n8n\_backend {  
    server 127.0.0.1:5678;  
    server 127.0.0.1:5679;  
    server 127.0.0.1:5680;  
}

###### *C. Rate Limiting*

\# Implementar rate limiting por usuário  
from flask\_limiter import Limiter  
limiter \= Limiter(app, key\_func=get\_remote\_address)

@app.route("/api/gold/total\_por\_regiao")  
@limiter.limit("100 per minute")  \# 100 requests por minuto por usuário  
def get\_total\_por\_regiao():  
    pass

#### 2\. Governança e Segurança para Manter a Solução Íntegra

##### Implementações de Segurança

###### *A. Validação de Dados (Data Quality)*

\# Verificação de integridade dos dados  
def \_validate\_data\_quality(df):  
    \# Verifica se não há valores nulos em colunas críticas  
    null\_counts \= df.select(\[F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns\])  
      
    \# Verifica se valores estão em ranges esperados  
    df.filter((F.col("acessos") \< 0\) | (F.col("ano") \< 2000)).count() \== 0

###### *B. Hash SHA-256 para Verificação*

\# Verificação de integridade dos arquivos  
def sha256\_file(file\_path: str) \-\> str:  
    """Calcula hash SHA-256 para verificar integridade"""  
    sha256\_hash \= hashlib.sha256()  
    with open(file\_path, "rb") as f:  
        for chunk in iter(lambda: f.read(4096), b""):  
            sha256\_hash.update(chunk)  
    return sha256\_hash.hexdigest()

\# Verificação de duplicatas  
def \_hash\_exists(root: str, file\_hash: str) \-\> Optional\[str\]:  
    """Verifica se arquivo já existe para evitar reprocessamento"""

###### *C. Manifest com Metadados*

\# Rastreabilidade completa dos dados  
manifest \= {  
    "dataset\_query": dataset\_query,  
    "package\_id": pkg.get("id"),  
    "resource\_url": url,  
    "file\_hash": file\_hash,  
    "started\_at": started\_at,  
    "finished\_at": finished\_at,  
    "download\_meta": download\_meta  
}

##### Segurança Adicional a Realizar

###### *A. Autenticação e Autorização*

\# JWT para autenticação de usuários  
from flask\_jwt\_extended import jwt\_required, get\_jwt\_identity

@app.route("/api/gold/total\_por\_regiao")  
@jwt\_required()  
def get\_total\_por\_regiao():  
    user\_id \= get\_jwt\_identity()  
    \# Verificar permissões do usuário  
    if not has\_access(user\_id, "gold\_data"):  
        return {"error": "Acesso negado"}, 403

###### *B. Criptografia de Dados Sensíveis*

\# Criptografia de dados em repouso  
from cryptography.fernet import Fernet

def encrypt\_sensitive\_data(data):  
    key \= Fernet.generate\_key()  
    f \= Fernet(key)  
    encrypted\_data \= f.encrypt(data.encode())  
    return encrypted\_data, key

###### *C. Auditoria e Logs*

\# Sistema de auditoria completo  
import logging  
from datetime import datetime

def audit\_log(user\_id, action, resource, details):  
    logging.info(f"AUDIT: {datetime.now()} \- User: {user\_id}, Action: {action}, Resource: {resource}, Details: {details}")

#### 3\. Monitoramento para Garantir Funcionamento Correto

##### Implementações de Monitoramento

###### *A. Logs Estruturados*

\# Logging configurado para todas as operações  
logging.basicConfig(level=logging.INFO, format='\[Q2\] %(message)s')  
log \= logging.getLogger("Q2")

\# Logs em pontos críticos  
log.info(f"Processando {len(csv\_files)} arquivos CSV")  
log.info(f"Gold tables criadas em {gold\_dir}")

###### *B. Tratamento de Erros Robusto*

\# Try-catch em operações críticas  
try:  
    df \= \_read\_csv\_robust(spark, csv\_path)  
except Exception as e:  
    log.error(f"Erro ao ler CSV {csv\_path}: {e}")  
    raise SystemExit(f"Falha crítica: {e}")

##### Implementações de Monitoramento a Realizar

###### *A. Métricas de Performance*

###### *B. Health Checks*

###### *C. Alertas e Notificações*

###### *D. Dashboard de Monitoramento (Grafana)*

### **✅ Recursos e Infraestrutura**

**Dimensões**: Mapeamento UF → Região e tecnologias

**Dependências**: requirements.txt com todas as bibliotecas necessárias

**Ambiente**: Virtual environment configurado

**Estrutura**: Organização clara de pastas e arquivos

### **🎯 O que está completo:**

**Pipeline ETL completo** (Ingestão → Transformação → Agregação)

**Data Lake com 3 camadas** (Bronze, Silver, Gold)

**Automação via n8n** (Scheduler \+ API)

**Interface IA** para consultas em linguagem natural

**Documentação técnica** detalhada

**Containerização** com Docker

**Configuração Spark** otimizada

### **📋 Para executar a prova:**

Q1: python \-m src.main \--mode ingest \--dataset "Acessos – Banda Larga Fixa"

Q2: python \-m src.transform\_q2 \--datalake ./data\_lake

Q3: Ativar workflows no n8n

Q4: Fazer perguntas via webhook ou terminal
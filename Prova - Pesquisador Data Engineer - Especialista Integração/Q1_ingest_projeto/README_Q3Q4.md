---

# Q3 & Q4 – n8n Workflows

Este documento explica como instalar, configurar e executar os workflows do n8n para automação ETL e IA.

---

## 1) Instalação do n8n

### Opção A: Via npm (recomendado)
```bash
# Instalar Node.js (se não tiver)
# Windows: https://nodejs.org/
# macOS: brew install node
# Linux: sudo apt install nodejs npm

# Instalar n8n globalmente
npm install n8n -g
```

### Opção B: Via Docker
```bash
docker run -it --rm \
  --name n8n \
  -p 5678:5678 \
  -v ~/.n8n:/home/node/.n8n \
  n8nio/n8n
```

---

## 2) Configuração e Execução

### Iniciar o n8n
```bash
n8n start
```

### Acessar a interface web
Abra o navegador e acesse: `http://localhost:5678`

### Importar os workflows
1. Na interface do n8n, clique em **"Import from file"**
2. Selecione os arquivos da pasta `Q3/workflows/`:
   - `etl_scheduler.json` (Q3 - Automação ETL)
   - `api_datalake.json` (Q3 - API REST)
   - `q4ia.json` (Q4 - IA com LLM)

---

## 3) Configuração dos Workflows

### Q3 - ETL Scheduler
- **Cron**: Configurado para executar diariamente às 02:00
- **Execução**: Chama scripts Python da Q1 e Q2
- **Status**: Ativo após importação

### Q3 - API REST
- **Endpoint**: `/api/datalake` (porta 5678)
- **Métodos**: GET para consultar dados
- **Exemplo**: `http://localhost:5678/api/datalake?query=acessos_fibra`

### Q4 - IA com LLM
- **Configuração**: Adicione sua API key do OpenAI
- **Endpoint**: `/api/ia` para perguntas em linguagem natural
- **Exemplo**: "Qual região teve maior crescimento em fibra?"

---

## 4) Teste dos Workflows

### Testar ETL
1. Execute manualmente o workflow "ETL Scheduler"
2. Verifique logs de execução
3. Confirme criação de dados em `data_lake/`

### Testar API
```bash
curl "http://localhost:5678/api/datalake?query=total_acessos"
```

### Testar IA
```bash
curl -X POST "http://localhost:5678/api/ia" \
  -H "Content-Type: application/json" \
  -d '{"question": "Qual região teve maior crescimento em fibra?"}'
```

---

## 5) Configuração de Produção

### Variáveis de Ambiente
Crie arquivo `.env` na raiz do projeto:
```bash
OPENAI_API_KEY=sua_chave_aqui
DATALAKE_PATH=./data_lake
PYTHON_PATH=./venv/bin/python
```

### Execução como Serviço
```bash
# Criar serviço systemd (Linux)
sudo systemctl enable n8n
sudo systemctl start n8n

# macOS (LaunchAgent)
brew services start n8n
```

---

## 6) Solução de Problemas

- **Erro: "n8n command not found"**
  → Reinstale com `npm install n8n -g`

- **Erro: "Port 5678 already in use"**
  → Mude a porta: `n8n start --port 5679`

- **Workflow não executa**
  → Verifique logs em `~/.n8n/logs/`

- **API não responde**
  → Confirme se o workflow está ativo e acessível
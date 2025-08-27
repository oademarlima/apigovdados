# Dockerfile simples com Spark local
FROM python:3.11-slim

# Dependências básicas
RUN apt-get update && apt-get install -y curl unzip && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src
COPY data_lake /app/data_lake

CMD ["python", "-m", "src.main", "--mode", "ingest", "--dataset", "Acessos – Banda Larga Fixa"]

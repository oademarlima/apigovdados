
# Guia de Instalação do Python e Pip no Windows

Este guia mostra como preparar o **Python 3** e o **pip** no Windows para rodar o projeto.

---

## 0) Versões do Java, Python e Pip que foi instalada no ambiente de testes

**Ambiente Testado:**
- **Java**: OpenJDK 17
- **Python**: 3.13.7  
- **pip**: 25.2

**Versões Mínimas Recomendadas:**
- **Java**: 17+ (LTS)
- **Python**: 3.9+ (recomendado 3.10+)
- **pip**: 21.0+

## 1) Verificar se já existe Python instalado
Abra o **Prompt de Comando (cmd)** e digite:
```bash
python --version
```
ou
```bash
py --version
```

Se aparecer **Python 3.9 ou superior**, já está ok. Se não, siga o próximo passo.

---

## 2) Instalar Python pelo site oficial
1. Baixe em: [https://www.python.org/downloads/windows/](https://www.python.org/downloads/windows/)
2. Execute o instalador e **marque a opção "Add Python to PATH"**.
3. Finalize a instalação.

Depois, feche e reabra o terminal e teste:
```bash
python --version
```

---

## 3) Conferir se o pip funciona
No terminal:
```bash
python -m pip --version
```
ou
```bash
py -m pip --version
```

Se der erro, siga o próximo passo.

---

## 4) Criar ou recuperar o pip
```bash
python -m ensurepip --upgrade
python -m pip install --upgrade pip setuptools wheel
```

---

## 5) Criar um ambiente virtual (recomendado)
Dentro da pasta do projeto:
```bash
python -m venv venv
venv\Scripts\activate
python -m pip install --upgrade pip
```

---

## 6) Instalar dependências do projeto
Com a venv ativa:
```bash
python -m pip install -r requirements.txt
```

✅ Pronto! Agora você já pode rodar os scripts do projeto.

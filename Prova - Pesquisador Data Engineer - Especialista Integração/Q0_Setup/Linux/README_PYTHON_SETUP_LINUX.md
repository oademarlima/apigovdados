# Guia de Instalação do Python e Pip no Linux

Este guia mostra como preparar o **Python 3** e o **pip** no Linux (Debian/Ubuntu/Fedora).

---

## 1) Verificar versão instalada
```bash
python3 --version
```

Se for menor que **3.9**, instale o Python mais recente.

---

## 2) Instalar Python + pip (Debian/Ubuntu)
```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
```

### Fedora/RHEL:
```bash
sudo dnf install -y python3 python3-pip python3-virtualenv
```

---

## 3) Conferir instalação
```bash
python3 --version
python3 -m pip --version
```

---

## 4) Criar ambiente virtual (recomendado)
Dentro da pasta do projeto:
```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
```

---

## 5) Instalar dependências do projeto
Com a venv ativa:
```bash
python -m pip install -r requirements.txt
```

✅ Pronto! Ambiente configurado no Linux.

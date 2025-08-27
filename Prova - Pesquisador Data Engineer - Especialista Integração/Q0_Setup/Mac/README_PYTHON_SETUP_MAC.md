# Guia de Instalação do Python e Pip no macOS

Este guia mostra como preparar o **Python 3** e o **pip** no macOS para rodar o projeto.

---

## 1) Verificar versão do Python instalada
No Terminal:
```bash
python3 --version
```
O projeto requer **Python 3.9 ou superior** (recomendado 3.10+).  
Se aparecer algo como `Python 3.9.6`, já está ok.

---

## 2) Verificar se o pip já funciona
No Terminal:
```bash
python3 -m pip --version
```

- Se aparecer uma versão, vá para o passo 4.  
- Se der erro, siga o próximo passo.

---

## 3) Criar ou recuperar o pip
Ainda no Terminal:
```bash
python3 -m ensurepip --upgrade
python3 -m pip --version
```

Isso instala ou atualiza o pip para sua versão do Python.  
Depois atualize:
```bash
python3 -m pip install --upgrade pip setuptools wheel
```

---

## 4) Garantir que o PATH enxergue o pip (opcional)
Se mesmo assim `pip3` não for reconhecido, adicione o caminho correto no seu `~/.zshrc` (padrão do macOS):

### Intel:
```bash
echo 'export PATH="/Library/Frameworks/Python.framework/Versions/3.9/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Apple Silicon (M1/M2/M3, via Homebrew):
```bash
echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

Depois teste:
```bash
python3 -m pip --version
```

---

## 5) Criar um ambiente virtual (recomendado)
Crie um ambiente virtual separado para o projeto:
```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
```

Com a venv ativada, use sempre:
```bash
python -m pip install <pacote>
```

---

## 6) Se nada funcionar → Reinstalar via Homebrew
Se os passos acima não resolverem:

1. Instale o Homebrew (se ainda não tiver):
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

2. Instale o Python mais recente:
```bash
brew install python
```

3. Teste:
```bash
python3 --version
python3 -m pip --version
```

---

✅ Pronto! Agora você pode instalar as dependências do projeto normalmente:
```bash
python -m pip install -r requirements.txt
```

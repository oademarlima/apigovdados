# Setup do Java (Windows) para rodar PySpark

Este guia explica como instalar e configurar o **Java 17 (LTS)** no Windows, requisito para rodar o PySpark.

---

## 1. Baixar e instalar o Java 17
- Acesse: https://adoptium.net/temurin/releases/?version=17
- Baixe o instalador para Windows (x64 MSI).
- Instale normalmente.

---

## 2. Configurar JAVA_HOME no Windows
1. Abra o menu **Iniciar** e pesquise por *variáveis de ambiente*.
2. Clique em **Editar variáveis de ambiente do sistema**.
3. Em **Variáveis do sistema**, clique em **Novo** e adicione:
   - Nome: `JAVA_HOME`
   - Valor: `C:\Program Files\Eclipse Adoptium\jdk-17` (ajuste conforme sua instalação)
4. Ainda em **Variáveis do sistema**, edite a variável `Path` e adicione:
   - `%JAVA_HOME%\bin`

---

## 3. Confirmar
Abra o **Prompt de Comando** ou **PowerShell** e rode:
```powershell
java -version
echo %JAVA_HOME%
```

Você deve ver a versão 17 do Java.

---

## 4. Rodar PySpark
No terminal (PowerShell ou CMD), dentro da sua venv:
```powershell
python -m src.transform_q2 --datalake ./data_lake
```

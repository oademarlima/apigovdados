# Setup do Java (macOS) para rodar PySpark

Este guia explica como instalar e configurar o **Java 17 (LTS)** no macOS, requisito para rodar o PySpark.

---

## 1. Verificar se o Java já está instalado
No terminal:
```bash
java -version
```

Se aparecer:
```
The operation couldn’t be completed. Unable to locate a Java Runtime.
```
significa que você **não tem Java instalado**.

---

## 2. Instalar o OpenJDK 17 com Homebrew
```bash
brew install openjdk@17
```

---

## 3. Configurar a variável JAVA_HOME

Liste as versões disponíveis:
```bash
/usr/libexec/java_home -V
```

Deve aparecer algo como:
```
17.0.16 (arm64) "Homebrew" ...
```

Agora configure o Java 17 no ambiente:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

Teste:
```bash
java -version
```
Deve mostrar algo como:
```
openjdk version "17.0.xx"
```

---

## 4. Tornar a configuração permanente

Edite o arquivo `~/.zshrc` (caso use zsh, padrão no macOS):
```bash
nano ~/.zshrc
```

Adicione ao final:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

Salve com `CTRL+O` → Enter → `CTRL+X`.  
Depois recarregue:
```bash
source ~/.zshrc
```

---

## 5. Confirmar
```bash
echo $JAVA_HOME
java -version
```

Se aparecer o caminho do Java 17 e a versão correta, está pronto ✅

---

## 6. Rodar PySpark
Agora já é possível rodar comandos como:
```bash
python -m src.transform_q2 --datalake ./data_lake
```

---

> 📌 Obs.: este guia é específico para **macOS com Homebrew**.  
Para Linux ou Windows, os passos são diferentes.

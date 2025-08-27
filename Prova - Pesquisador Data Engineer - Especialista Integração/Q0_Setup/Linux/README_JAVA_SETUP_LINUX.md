# Setup do Java (Linux) para rodar PySpark

Este guia explica como instalar e configurar o **Java 17 (LTS)** no Linux, requisito para rodar o PySpark.

---

## 1. Verificar se o Java já está instalado
```bash
java -version
```
Se não encontrar ou mostrar versão antiga, instale o Java 17.

---

## 2. Instalar o OpenJDK 17
### Ubuntu/Debian
```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
```

### Fedora/RHEL/CentOS
```bash
sudo dnf install java-17-openjdk-devel -y
```

---

## 3. Configurar JAVA_HOME (opcional, mas recomendado)
Descubra onde está o Java:
```bash
sudo update-alternatives --config java
```
ou
```bash
readlink -f $(which java)
```

Adicione ao `~/.bashrc` ou `~/.zshrc`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Recarregue:
```bash
source ~/.bashrc
```

---

## 4. Confirmar
```bash
echo $JAVA_HOME
java -version
```

---

## 5. Rodar PySpark
```bash
python -m src.transform_q2 --datalake ./data_lake
```

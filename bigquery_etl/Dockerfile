# 1. Imagem base do Python
FROM python:3.11-slim

# 2. Criar diretório de trabalho dentro do container
WORKDIR /app

# 3. Copiar os arquivos do projeto
COPY . /app

# 4. Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt

# 5. Definir o comando padrão (rodar o ETL)
CMD ["python", "main.py"]

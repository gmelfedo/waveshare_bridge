ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.11-alpine3.19
FROM ${BUILD_FROM}

# Imposta la cartella di lavoro
WORKDIR /app

# Installa le dipendenze di sistema necessarie
RUN apk add --no-cache build-base

# Copia e installa i requisiti Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice dello script
COPY main.py .

# Avvia l'applicazione
CMD [ "python", "-u", "./main.py" ]
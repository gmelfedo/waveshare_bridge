FROM python:alpine

# Imposta la cartella di lavoro
WORKDIR /app

# Installa le dipendenze di sistema necessarie per compilazioni eventuali
RUN apk add --no-cache build-base

# Copia e installa i requisiti Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice dello script
COPY main.py .

# Avvia l'applicazione in modalità unbuffered per vedere i log in tempo reale
CMD [ "python", "-u", "./main.py" ]
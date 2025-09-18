FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt 

COPY . .

# Installer cron
RUN apt-get update && apt-get install -y cron

# Copier le fichier cron
COPY binance_cron /etc/cron.d/binance_cron
RUN chmod 0644 /etc/cron.d/binance_cron && crontab /etc/cron.d/binance_cron

# Lancer cron en premier plan
CMD ["cron", "-f"]

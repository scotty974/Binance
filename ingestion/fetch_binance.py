import requests
import pandas as pd
import os
import json
import time
from hdfs import InsecureClient

# Connexion WebHDFS
client = InsecureClient("http://namenode:9870", user="hadoop")

# URL Binance
BINANCE_URL = "https://api.binance.com/api/v3/ticker/24hr"

# Dossiers locaux et HDFS
LOCAL_DIR = "data/raw"
HDFS_DIR = "/binance/raw"

# Attente entre les vérifications du safe mode (en secondes)
SAFE_MODE_CHECK_INTERVAL = 5

def fetch_binance():
    """Récupère les données Binance et retourne le JSON"""
    try:
        response = requests.get(BINANCE_URL, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"[ERROR] Impossible de récupérer les données Binance: {e}")
        return None

def save_local(data):
    """Sauvegarde les données localement"""
    os.makedirs(LOCAL_DIR, exist_ok=True)
    timestamp = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"{LOCAL_DIR}/binance_{timestamp}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return filename, timestamp

def wait_for_namenode():
    """Attend la fin du safe mode du NameNode"""
    import requests
    safe_mode_url = "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
    while True:
        try:
            r = requests.get(safe_mode_url, timeout=10).json()
            safe_mode = r['beans'][0]['Safemode']
            if safe_mode == "OFF":
                print("[INFO] NameNode est sorti du safe mode.")
                break
            else:
                print("[INFO] NameNode en safe mode, attente...")
        except Exception as e:
            print(f"[WARN] Impossible de vérifier le safe mode: {e}")
        time.sleep(SAFE_MODE_CHECK_INTERVAL)

def save_hdfs(local_file, timestamp):
    """Sauvegarde les données sur HDFS après safe mode"""
    wait_for_namenode()
    try:
        client.makedirs(HDFS_DIR)
        hdfs_path = f"{HDFS_DIR}/binance_{timestamp}.json"
        with open(local_file, "r", encoding="utf-8") as f:
            content = f.read()
        with client.write(hdfs_path, encoding="utf-8", overwrite=True) as writer:
            writer.write(content)
        print(f"[INFO] Données sauvegardées sur HDFS: {hdfs_path}")
    except Exception as e:
        print(f"[ERROR] Impossible d'écrire sur HDFS: {e}")

if __name__ == "__main__":
    data = fetch_binance()
    if data:
        local_file, timestamp = save_local(data)
        save_hdfs(local_file, timestamp)
    else:
        print("[WARN] Pas de données récupérées.")

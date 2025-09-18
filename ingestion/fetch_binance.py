import requests
import pandas as pd 
import os 
import json
from hdfs import InsecureClient

# Connexion WebHDFS
client = InsecureClient("http://namenode:9870", user="hadoop")

def fetch_binance():
    url = "https://api.binance.com/api/v3/ticker/24hr"
    response = requests.get(url)
    return response.json()

if __name__ == "__main__":
    data = fetch_binance()
    
    # Local save
    os.makedirs("data/raw", exist_ok=True)
    timestamp = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"data/raw/binance_{timestamp}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
        
    # HDFS path complet
    hdfs_path = f"/binance/raw/binance_{timestamp}.json"
    
    # Création du dossier si besoin
    client.makedirs('/binance/raw')
    
    # Envoi du contenu du fichier
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
        writer.write(content)
    
    print(f"Data saved to HDFS at {hdfs_path}")

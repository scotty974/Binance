#!/usr/bin/env python3
"""
Stockage HDFS direct - Méthode la plus simple
"""

import requests
import json
import subprocess
import os
from datetime import datetime

def store_direct():
    """
    Stockage direct via stdin
    """
    print("🔄 HDFS DIRECT STORAGE")
    print("=" * 30)
    
    try:
        # 1. Récupérer les données
        print("📡 Fetching data...")
        url = "https://api.binance.com/api/v3/ticker/24hr"
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            print(f"❌ API Error: {response.status_code}")
            return False
        
        data = response.json()
        print(f"   ✅ {len(data)} symbols fetched")
        
        # 2. Créer le répertoire HDFS
        print("📁 Creating directory...")
        subprocess.run([
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-mkdir", "-p", "/binance/raw"
        ], check=True, capture_output=True)
        
        # 3. Stocker via pipe (stdin)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hdfs_file = f"/binance/raw/binance_{timestamp}.json"
        
        print("💾 Storing to HDFS...")
        
        # Convertir en JSON
        json_data = json.dumps(data, indent=2)
        
        # Utiliser hdfs dfs -put avec stdin
        process = subprocess.Popen([
            "docker", "exec", "-i", "namenode",
            "hdfs", "dfs", "-put", "-", hdfs_file
        ], stdin=subprocess.PIPE, text=True)
        
        # Envoyer les données
        process.communicate(input=json_data)
        
        if process.returncode == 0:
            print(f"   ✅ Success: {hdfs_file}")
        else:
            print("   ❌ Failed to store")
            return False
        
        # 4. Vérifier
        print("🔍 Verification...")
        result = subprocess.run([
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-ls", "/binance/raw/"
        ], capture_output=True, text=True)
        
        print("📂 Files:")
        for line in result.stdout.strip().split('\n'):
            if 'binance_' in line:
                print(f"   {line}")
        
        print("\n" + "=" * 30)
        print("🎉 HDFS STORAGE SUCCESS!")
        print("=" * 30)
        print(f"📁 Path: {hdfs_file}")
        print("🔥 Run: python binance_spark_analytics.py --hdfs")
        
        return hdfs_file
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    store_direct()

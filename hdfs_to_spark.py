#!/usr/bin/env python3
"""
Récupère les données depuis HDFS et lance l'analyse Spark
"""

import subprocess
import json
import tempfile
import os

def hdfs_to_spark_analysis():
    """
    Workflow complet : HDFS → Spark Analytics
    """
    print("🔄 HDFS → SPARK WORKFLOW")
    print("=" * 40)
    
    try:
        # 1. Lister les fichiers HDFS
        print("📂 Listing HDFS files...")
        result = subprocess.run([
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-ls", "/binance/raw/"
        ], capture_output=True, text=True)
        
        print("Available files:")
        files = []
        for line in result.stdout.strip().split('\n'):
            if 'binance_' in line:
                filename = line.split()[-1]  # Dernier élément = nom du fichier
                files.append(filename)
                print(f"   {filename}")
        
        if not files:
            print("❌ No files found in HDFS")
            return False
        
        # 2. Prendre le fichier le plus récent
        latest_file = sorted(files)[-1]
        print(f"\n📥 Using latest file: {latest_file}")
        
        # 3. Récupérer le contenu via hdfs dfs -cat
        print("📖 Reading file content...")
        content_result = subprocess.run([
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-cat", latest_file
        ], capture_output=True, text=True)
        
        if content_result.returncode != 0:
            print("❌ Failed to read HDFS file")
            return False
        
        # 4. Parser le JSON
        print("🔍 Parsing JSON data...")
        try:
            data = json.loads(content_result.stdout)
            print(f"   ✅ {len(data)} records parsed")
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing failed: {e}")
            return False
        
        # 5. Sauvegarder temporairement en local
        temp_file = "hdfs_retrieved_data.json"
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Data saved locally: {temp_file}")
        
        # 6. Lancer l'analyse Spark avec ce fichier
        print("\n🔥 LAUNCHING SPARK ANALYSIS...")
        print("=" * 40)
        
        spark_result = subprocess.run([
            "python", "binance_spark_analytics.py", 
            "--json-file", temp_file
        ], capture_output=True, text=True)
        
        # Afficher les résultats
        print(spark_result.stdout)
        if spark_result.stderr:
            print("Errors:", spark_result.stderr)
        
        # 7. Nettoyer
        os.remove(temp_file)
        
        if spark_result.returncode == 0:
            print("\n" + "=" * 40)
            print("🎉 HDFS → SPARK WORKFLOW SUCCESS!")
            print("=" * 40)
            print("✅ Data retrieved from HDFS")
            print("✅ Spark analysis completed")
            print("✅ Results saved to MongoDB")
            return True
        else:
            print("❌ Spark analysis failed")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    hdfs_to_spark_analysis()

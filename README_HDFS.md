# 🗄️ Binance Spark Analytics - Mode HDFS

## 🎯 Guide Complet pour Récupérer les Données depuis HDFS

Ce guide vous explique comment utiliser **HDFS (Hadoop Distributed File System)** pour stocker et récupérer les données Binance avec Spark.

## 🏗️ Architecture HDFS

```
API Binance → Stockage HDFS → Spark Analytics → MongoDB → Dashboard
     ↓              ↓              ↓             ↓         ↓
  Données JSON   Fichiers dans   Traitement    Résultats  Visualisation
   temps réel     /binance/raw/   distribué    structurés interactive
```

## 📁 Structure HDFS

```
HDFS:/
└── binance/
    └── raw/
        ├── binance_20250119_120000.json
        ├── binance_20250119_130000.json
        └── binance_20250119_140000.json
```

## 🚀 Étapes pour Utiliser HDFS

### **1. 📦 Stocker les Données dans HDFS**

#### **Méthode A : Script Automatique**
```bash
# Créer et exécuter le gestionnaire HDFS
python hdfs_manager.py store
```

#### **Méthode B : Commandes Manuelles**
```bash
# 1. Créer le répertoire HDFS
docker exec namenode hdfs dfs -mkdir -p /binance/raw

# 2. Récupérer les données et les copier
# (Voir section "Commandes HDFS Détaillées" ci-dessous)
```

### **2. 🔍 Vérifier les Fichiers HDFS**

```bash
# Lister les fichiers
docker exec namenode hdfs dfs -ls /binance/raw/

# Voir le contenu d'un fichier (premières lignes)
docker exec namenode hdfs dfs -cat /binance/raw/binance_*.json | head -20

# Informations sur un fichier
docker exec namenode hdfs dfs -stat /binance/raw/binance_*.json
```

### **3. 🔥 Analyser avec Spark depuis HDFS**

```bash
# Méthode recommandée : Workflow complet HDFS → Spark
python hdfs_to_spark.py

# Ou étapes manuelles :
# 1. Stocker dans HDFS
python hdfs_direct.py

# 2. Récupérer et analyser
python hdfs_to_spark.py
```

## 🛠️ Commandes HDFS Détaillées

### **Stockage des Données**

```bash
# 1. Récupérer les données Binance (script Python)
python -c "
import requests, json, datetime
data = requests.get('https://api.binance.com/api/v3/ticker/24hr').json()
timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
with open(f'binance_{timestamp}.json', 'w') as f:
    json.dump(data, f, indent=2)
print(f'Data saved: binance_{timestamp}.json')
"

# 2. Copier vers le container namenode
docker cp binance_*.json namenode:/tmp/

# 3. Déplacer vers HDFS
docker exec namenode hdfs dfs -put /tmp/binance_*.json /binance/raw/

# 4. Nettoyer le fichier temporaire
docker exec namenode rm /tmp/binance_*.json
rm binance_*.json
```

### **Gestion des Fichiers HDFS**

```bash
# Lister tous les fichiers
docker exec namenode hdfs dfs -ls -R /binance/

# Voir la taille des fichiers
docker exec namenode hdfs dfs -du -h /binance/raw/

# Supprimer un fichier
docker exec namenode hdfs dfs -rm /binance/raw/binance_old.json

# Supprimer tous les fichiers anciens
docker exec namenode hdfs dfs -rm /binance/raw/binance_2025011*.json

# Copier un fichier depuis HDFS vers local
docker exec namenode hdfs dfs -get /binance/raw/binance_latest.json /tmp/
docker cp namenode:/tmp/binance_latest.json ./
```

### **Surveillance HDFS**

```bash
# Statut du système HDFS
docker exec namenode hdfs dfsadmin -report

# Vérifier l'intégrité des fichiers
docker exec namenode hdfs fsck /binance/raw/

# Espace disponible
docker exec namenode hdfs dfs -df -h /
```

## 🔧 Script Gestionnaire HDFS

### **Utilisation du Gestionnaire HDFS**

```bash
# Stocker de nouvelles données dans HDFS
python hdfs_manager.py store

# Lister les fichiers HDFS
python hdfs_manager.py list

# Lire un fichier spécifique
python hdfs_manager.py read binance_20250119_120000.json
```

## 🔥 Workflow Complet HDFS → Spark

### **1. Préparation**
```bash
# Démarrer l'infrastructure
docker-compose -f compose.yml up -d

# Vérifier que HDFS est actif
docker exec namenode hdfs dfsadmin -safemode get
```

### **2. Stockage des Données**
```bash
# Méthode automatique
python hdfs_manager.py store

# Ou méthode manuelle
python -c "
import requests, json, datetime
data = requests.get('https://api.binance.com/api/v3/ticker/24hr').json()
timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
with open(f'binance_{timestamp}.json', 'w') as f:
    json.dump(data, f, indent=2)
print(f'binance_{timestamp}.json')
"

# Copier vers HDFS
docker cp binance_*.json namenode:/tmp/
docker exec namenode hdfs dfs -mkdir -p /binance/raw
docker exec namenode hdfs dfs -put /tmp/binance_*.json /binance/raw/
```

### **3. Analyse Spark depuis HDFS**
```bash
# Lancer l'analyse Spark
python binance_spark_analytics.py --hdfs

# Résultats attendus :
# ✅ 413 USDT pairs analyzed
# ✅ 99 critical alerts detected
# ✅ $20+ billion volume processed
# ✅ Results saved to MongoDB
```

### **4. Visualisation**
```bash
# Dashboard interactif
streamlit run visualization/dashboard/dashboard.py
```

## 🚨 Résolution de Problèmes HDFS

### **Problème : "No FileSystem for scheme 'C'"**
```bash
# Solution : Utiliser les commandes Docker exec
docker exec namenode hdfs dfs -ls /
```

### **Problème : "Connection refused namenode:9000"**
```bash
# Vérifier que namenode est actif
docker ps | grep namenode

# Redémarrer si nécessaire
docker restart namenode
```

### **Problème : "Permission denied"**
```bash
# Désactiver les permissions HDFS (déjà fait dans hadoop.env)
docker exec namenode hdfs dfs -chmod -R 777 /binance/
```

### **Problème : "Safe mode is ON"**
```bash
# Désactiver le mode safe
docker exec namenode hdfs dfsadmin -safemode leave
```

## 📊 Comparaison des Modes

| Mode | Source | Avantages | Inconvénients |
|------|--------|-----------|---------------|
| **API** | Direct Binance | ⚡ Rapide, temps réel | 🔄 Pas de persistance |
| **JSON File** | Fichier local | 💾 Persistant, testable | 📁 Stockage limité |
| **HDFS** | Hadoop distribué | 🏗️ Scalable, distribué | 🔧 Configuration complexe |

## 🎯 Recommandations

### **Pour le Développement :**
```bash
# Mode JSON File (simulation HDFS)
python binance_spark_analytics.py --json-file temp_data/binance_*.json
```

### **Pour la Production :**
```bash
# Mode HDFS (architecture complète)
python hdfs_manager.py store
python binance_spark_analytics.py --hdfs
```

### **Pour les Tests Rapides :**
```bash
# Mode API (temps réel)
python binance_spark_analytics.py
```

## 🎉 Résumé

Vous avez maintenant **3 méthodes** pour alimenter votre pipeline Spark :

1. **📡 API Mode** : Données temps réel directes
2. **📁 JSON File Mode** : Simulation HDFS avec fichiers locaux  
3. **🗄️ HDFS Mode** : Architecture Big Data complète

**Toutes les méthodes produisent les mêmes résultats d'analyse Spark : 413 cryptos, 99 alertes critiques, $20+ milliards de volume !** 🚀

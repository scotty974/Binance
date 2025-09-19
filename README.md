# 🔥 Binance Spark Analytics

## 🎯 Description

Système d'analyse avancée des cryptomonnaies Binance utilisant Apache Spark pour :
- 📊 **Traitement distribué** de données crypto en temps réel
- 🔍 **Détection d'anomalies** automatique (pump & dump, wash trading)
- 📈 **Calcul d'indicateurs techniques** (volatilité, momentum, signaux)
- 🚨 **Alertes critiques** pour les mouvements suspects
- 📊 **Dashboard interactif** pour visualisation

## 🏗️ Architecture

```
API Binance → Spark Analytics → MongoDB → Dashboard
     ↓              ↓              ↓         ↓
  Données        Nettoyage +   Collections  Visualisation
   brutes        Indicateurs +  structurées interactive
                 Anomalies
```

## 📁 Structure du Projet

```
Binance/
├── 🔥 ANALYSE PRINCIPALE
│   ├── binance_spark_analytics.py    # Script principal d'analyse
│   └── visualization/dashboard/      # Dashboard Streamlit
│       └── dashboard.py
│
├── 🏗️ INFRASTRUCTURE
│   ├── compose.yml                   # Docker Hadoop + MongoDB
│   ├── hadoop.env                    # Configuration Hadoop
│   └── requirements.txt              # Dépendances Python
│
├── 📊 JOBS SPARK (Votre Mission)
│   └── processing/
│       ├── spark_jobs/
│       │   ├── cleaning.py           # Nettoyage des données
│       │   ├── indicators.py         # Indicateurs techniques
│       │   └── anomalies.py          # Détection d'anomalies
│       └── pipelines/
│           └── pipeline_etl.py       # Pipeline ETL complet
│
└── 🔄 INGESTION (Fait par collègue)
    └── ingestion/
        ├── fetch_binance.py          # API → HDFS
        └── inser_mongo.py            # MongoDB insert
```

## 🚀 Installation et Lancement

### 1. **Prérequis**
```bash
# Installer Docker et Docker Compose
# Python 3.8+ avec pip
```

### 2. **Installation des dépendances**
```bash
pip install -r requirements.txt
```

### 3. **Lancer l'infrastructure**
```bash
# Démarrer Hadoop + MongoDB
docker-compose -f compose.yml up -d

# Vérifier que les containers sont actifs
docker ps
```

### 4. **Lancer l'analyse Spark**
```bash
# Analyse complète des cryptos
python binance_spark_analytics.py
```

### 5. **Visualiser les résultats**
```bash
# Dashboard interactif
streamlit run visualization/dashboard/dashboard.py
```

## 📊 Résultats d'Analyse

### **Données Traitées**
- **413 paires USDT** analysées en temps réel
- **99 alertes critiques** détectées automatiquement
- **$21+ milliards** de volume analysé
- **Signaux de trading** générés (BUY/SELL/HOLD)

### **Collections MongoDB**
- `processed_data` : Données analysées avec indicateurs
- `critical_alerts` : Alertes d'anomalies critiques
- `market_metrics` : Résumés et statistiques de marché

### **Indicateurs Calculés**
- **Volatilité** : Écart entre prix haut/bas
- **Momentum** : Variation de prix sur 24h
- **Score de risque** : Composite (volatilité + spread + momentum)
- **Signaux de trading** : STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
- **Niveaux d'anomalie** : NORMAL, LOW, MODERATE, HIGH, CRITICAL

## 🔍 Détection d'Anomalies

### **Types d'Anomalies Détectées**
- 🚨 **Mouvements de prix extrêmes** (>10% en 24h)
- 💰 **Volumes anormaux** (très élevés ou très faibles)
- 📊 **Spreads suspects** (manipulation potentielle)
- 🎭 **Patterns de pump & dump**
- 🔄 **Wash trading** (volume artificiel)

### **Recommandations Automatiques**
- `BUY_RECOMMENDED` : Signal d'achat avec faible risque
- `SELL_RECOMMENDED` : Signal de vente avec faible risque
- `CAUTION` : Anomalie modérée détectée
- `AVOID` : Anomalie critique - éviter
- `MONITOR` : Surveillance recommandée

## 📈 Dashboard Interactif

### **Fonctionnalités**
- 📊 **Vue d'ensemble** : Métriques globales en temps réel
- ⚖️ **Analyse des risques** : Distribution par catégories
- 🎯 **Signaux de trading** : Répartition BUY/SELL/HOLD
- 🔍 **Anomalies détectées** : Alertes critiques récentes
- 📈 **Performance** : Top gainers/losers
- 📋 **Données détaillées** : Table filtrable

### **Accès**
- **URL** : http://localhost:8501
- **Actualisation** : Bouton "Actualiser les Données"
- **Filtres** : Par risque, signal, anomalie

## 🔧 Configuration

### **Ports Utilisés**
- `27018` : MongoDB (externe)
- `8501` : Dashboard Streamlit
- `9870` : Hadoop NameNode
- `9000` : Hadoop HDFS

### **Base de Données**
- **MongoDB** : `binance_analytics`
- **Connexion** : `mongodb://localhost:27018/`

## 🎯 Utilisation Avancée

### **Pipeline ETL Complet (avec Spark)**
```bash
# Utiliser le pipeline ETL avec HDFS
python processing/pipelines/pipeline_etl.py --hdfs-path /binance/raw/data.json
```

### **Jobs Spark Individuels**
```bash
# Test du nettoyage
python processing/spark_jobs/cleaning.py

# Test des indicateurs
python processing/spark_jobs/indicators.py

# Test des anomalies
python processing/spark_jobs/anomalies.py
```

## 📊 Exemples de Résultats

### **Analyse Typique**
```
📊 ANALYSIS RESULTS:
Total symbols analyzed: 413
Critical alerts: 99
Average price change: 1.07%
Total volume: $21,315,935,388

Risk distribution:
LOW: 262, MEDIUM: 140, HIGH: 8, VERY_HIGH: 3

Signal distribution:
HOLD: 361, BUY: 40, SELL: 11, STRONG_BUY: 1

Top gainers: BARDUSDT (+162%), WUSDT (+21%)
Top losers: PROMUSDT (-8%), PUMPUSDT (-7%)
```

### **Alertes Critiques**
```
🚨 CRITICAL ALERTS:
BTCUSDT: AVOID - CRITICAL anomaly
ETHUSDT: AVOID - CRITICAL anomaly
BARDUSDT: CAUTION - HIGH anomaly (+162%)
```

## 🛠️ Maintenance

### **Redémarrer l'infrastructure**
```bash
docker-compose -f compose.yml down
docker-compose -f compose.yml up -d
```

### **Nettoyer les données**
```bash
# Se connecter à MongoDB et vider les collections
docker exec -it db mongo binance_analytics --eval "db.processed_data.deleteMany({})"
```

### **Logs et Debug**
```bash
# Voir les logs des containers
docker logs namenode
docker logs db

# Logs Python avec niveau DEBUG
export PYTHONPATH=.
python -c "import logging; logging.basicConfig(level=logging.DEBUG)"
```

## 🎉 Résumé

Ce système vous permet d'analyser **automatiquement** les marchés crypto avec :
- ✅ **Détection d'anomalies** en temps réel
- ✅ **Signaux de trading** automatiques
- ✅ **Dashboard interactif** pour visualisation
- ✅ **Architecture Spark** scalable
- ✅ **Alertes critiques** pour éviter les pièges

**Votre mission Spark est complète et opérationnelle !** 🚀

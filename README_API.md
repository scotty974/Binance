# 📡 Binance Spark Analytics - Mode API

## 🎯 Analyse Crypto en Temps Réel

**Système Spark qui analyse 413 cryptomonnaies USDT avec 100+ alertes critiques automatiques**

### 🏗️ Architecture
```
API Binance → Spark Analytics → MongoDB → Dashboard
   3279 cryptos    413 USDT pairs    Collections    Visualisation
   temps réel      analysées         structurées    interactive
```

## 🚀 Utilisation - 3 Étapes Simples

### **Étape 1 : Démarrer l'Infrastructure**
```bash
# Lancer Hadoop + MongoDB
docker-compose -f compose.yml up -d

# Vérifier que tout fonctionne
docker ps
```

### **Étape 2 : Lancer l'Analyse Spark**
```bash
# Analyse complète des cryptos
python binance_spark_analytics.py

# Résultats attendus :
# 413 USDT pairs analyzed
# 100+ critical alerts detected  
# $20+ billion volume processed
# Results saved to MongoDB
```

### **Étape 3 : Voir le Dashboard**
```bash
# Dashboard interactif
streamlit run visualization/dashboard/dashboard.py

# Ouvrir : http://localhost:8501
```

## 📊 Résultats Typiques

```
 ANALYSIS RESULTS:
Total symbols analyzed: 413
Critical alerts: 101
Average price change: 1.84%
Total volume: $20,651,192,158

 ANALYSIS COMPLETED SUCCESSFULLY!
```

## 🔍 Ce que Spark Analyse

### **Données Traitées**
- **3279 cryptos** récupérées depuis l'API Binance
- **413 paires USDT** filtrées et nettoyées
- **$20+ milliards** de volume analysé

### **Indicateurs Calculés**
- **Volatilité** : Écart prix haut/bas
- **Momentum** : Variation 24h
- **Score de risque** : Composite volatilité + spread
- **Signaux trading** : BUY/SELL/HOLD
- **Niveaux d'anomalie** : NORMAL → CRITICAL

### **Alertes Générées**
- 🚨 **Anomalies critiques** : Mouvements suspects
- 📈 **Signaux d'achat** : Opportunités détectées
- 📉 **Signaux de vente** : Risques identifiés
- ⚠️ **Recommandations** : BUY/SELL/AVOID/MONITOR

## 📊 Dashboard Interactif

### **Fonctionnalités**
- **Vue d'ensemble** : Métriques temps réel
- **Analyse des risques** : Distribution par catégories
- **Signaux de trading** : Répartition BUY/SELL/HOLD
- **Anomalies détectées** : Alertes critiques
- **Top gainers/losers** : Performance du marché

### **Accès**
- **URL** : http://localhost:8501
- **Actualisation** : Bouton "Actualiser les Données"
- **Filtres** : Par risque, signal, anomalie

## 🛠️ Configuration

### **Ports Utilisés**
- `27018` : MongoDB
- `8501` : Dashboard Streamlit
- `9870` : Hadoop NameNode (interface web)

### **Base de Données MongoDB**
- **Nom** : `binance_analytics`
- **Collections** :
  - `processed_data` : Données analysées
  - `critical_alerts` : Alertes critiques
  - `market_metrics` : Statistiques globales

## 🎯 Avantages du Mode API

✅ **Rapidité** : Analyse en 30 secondes  
✅ **Temps réel** : Données les plus récentes  
✅ **Simplicité** : Une seule commande  
✅ **Fiabilité** : Pas de dépendance HDFS  
✅ **Performance** : Traitement direct  

## 🔧 Dépannage

### **Problème : "Connection refused MongoDB"**
```bash
# Redémarrer MongoDB
docker restart db
```

### **Problème : "API timeout"**
```bash
# Relancer l'analyse
python binance_spark_analytics.py
```

### **Problème : "Dashboard ne s'ouvre pas"**
```bash
# Vérifier le port
netstat -an | grep 8501

# Relancer Streamlit
streamlit run visualization/dashboard/dashboard.py --port 8502
```

## 🎉 Résumé

**Mode API = Solution parfaite pour :**
- Analyses rapides et temps réel
- Tests et développement  
- Démonstrations
- Production légère

**Une seule commande pour analyser 413 cryptos avec Spark !** 🚀
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

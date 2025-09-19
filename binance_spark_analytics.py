#!/usr/bin/env python3
"""
Binance Spark Analytics - Pipeline Principal
Analyse des cryptomonnaies avec détection d'anomalies et signaux de trading
"""

import json
import requests
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceSparkAnalytics:
    def __init__(self, mongo_port=27018, use_hdfs=False, json_file=None):
        """
        Analyseur Spark pour données Binance
        """
        self.mongo_uri = f"mongodb://localhost:{mongo_port}/"
        self.db_name = "binance_analytics"
        self.use_hdfs = use_hdfs
        self.json_file = json_file
        self.hdfs_path = "hdfs://localhost:9000/binance/raw/"
        
    def fetch_binance_data(self):
        """
        Récupère les données depuis l'API Binance, HDFS ou fichier JSON local
        """
        if self.json_file:
            return self.load_from_json_file()
        elif self.use_hdfs:
            return self.load_from_hdfs()
        else:
            return self.fetch_from_api()
    
    def fetch_from_api(self):
        """
        Récupère les données depuis l'API Binance
        """
        logger.info("📡 Fetching Binance market data from API...")
        
        try:
            url = "https://api.binance.com/api/v3/ticker/24hr"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"   ✅ {len(data)} symbols fetched from API")
                return data
            else:
                logger.error(f"❌ API Error: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error fetching from API: {e}")
            return None
    
    def load_from_hdfs(self):
        """
        Charge les données depuis HDFS avec Spark
        """
        logger.info("📥 Loading Binance data from HDFS...")
        
        try:
            from pyspark.sql import SparkSession
            
            # Initialiser Spark avec configuration HDFS
            spark = SparkSession.builder \
                .appName("BinanceHDFSLoader") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "false") \
                .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "false") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            
            # Lire les fichiers JSON depuis HDFS
            logger.info(f"   📂 Reading from: {self.hdfs_path}")
            
            try:
                # Essayer de lire tous les fichiers JSON
                df = spark.read.option("multiline", "true").json(f"{self.hdfs_path}*.json")
                
                if df.count() > 0:
                    # Convertir en liste de dictionnaires (comme l'API)
                    data = df.collect()
                    data_list = [row.asDict() for row in data]
                    
                    logger.info(f"   ✅ {len(data_list)} records loaded from HDFS")
                    spark.stop()
                    return data_list
                else:
                    logger.warning("   ⚠️ No data found in HDFS")
                    spark.stop()
                    return None
                    
            except Exception as hdfs_error:
                logger.error(f"   ❌ HDFS read error: {hdfs_error}")
                
                # Fallback: essayer un fichier spécifique
                try:
                    logger.info("   🔄 Trying specific file pattern...")
                    df = spark.read.option("multiline", "true").json(f"{self.hdfs_path}binance_*.json")
                    
                    if df.count() > 0:
                        data = df.collect()
                        data_list = [row.asDict() for row in data]
                        logger.info(f"   ✅ {len(data_list)} records loaded from specific files")
                        spark.stop()
                        return data_list
                        
                except Exception as e2:
                    logger.error(f"   ❌ Fallback failed: {e2}")
                
                spark.stop()
                return None
                
        except Exception as e:
            logger.error(f"❌ Error loading from HDFS: {e}")
            return None
    
    def load_from_json_file(self):
        """
        Charge les données depuis un fichier JSON local (simulation HDFS)
        """
        logger.info(f"📁 Loading Binance data from JSON file: {self.json_file}")
        
        try:
            import glob
            
            # Résoudre les wildcards si nécessaire
            if '*' in self.json_file:
                matching_files = glob.glob(self.json_file)
                if not matching_files:
                    logger.error(f"❌ No files found matching pattern: {self.json_file}")
                    return None
                
                # Prendre le fichier le plus récent
                actual_file = sorted(matching_files)[-1]
                logger.info(f"   📂 Using file: {actual_file}")
            else:
                actual_file = self.json_file
            
            with open(actual_file, 'r') as f:
                data = json.load(f)
            
            logger.info(f"   ✅ {len(data)} records loaded from JSON file")
            return data
            
        except Exception as e:
            logger.error(f"❌ Error loading JSON file: {e}")
            return None
    
    def clean_and_filter_data(self, data):
        """
        Nettoyage et filtrage des données (logique Spark)
        """
        logger.info("🧹 Cleaning and filtering data...")
        
        df = pd.DataFrame(data)
        
        # Convertir les types numériques
        numeric_columns = ['lastPrice', 'priceChangePercent', 'volume', 'bidPrice', 'askPrice', 
                          'openPrice', 'highPrice', 'lowPrice', 'quoteVolume']
        
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Filtrer les paires USDT avec volume significatif
        df_filtered = df[
            (df['symbol'].str.endswith('USDT')) &
            (df['lastPrice'] > 0) &
            (df['volume'] > 10000) &
            (df['bidPrice'] > 0) &
            (df['askPrice'] > df['bidPrice'])
        ].copy()
        
        logger.info(f"   ✅ {len(df_filtered)} USDT pairs after filtering")
        return df_filtered
    
    def calculate_technical_indicators(self, df):
        """
        Calcul des indicateurs techniques (logique Spark)
        """
        logger.info("📈 Calculating technical indicators...")
        
        # Indicateurs de base
        df['spread'] = df['askPrice'] - df['bidPrice']
        df['spread_percent'] = (df['spread'] / df['lastPrice']) * 100
        df['volatility'] = (df['highPrice'] - df['lowPrice']) / df['openPrice']
        df['volume_usd'] = df['volume'] * df['lastPrice']
        df['momentum'] = df['priceChangePercent'] / 100
        
        # Score de risque composite
        df['risk_score'] = (
            df['volatility'] * 0.4 + 
            df['spread_percent'] * 0.3 + 
            abs(df['momentum']) * 0.3
        )
        
        # Catégories de risque
        df['risk_category'] = pd.cut(
            df['risk_score'], 
            bins=[-float('inf'), 0.05, 0.15, 0.3, float('inf')],
            labels=['LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH']
        )
        
        # Signaux de trading
        conditions = [
            (df['momentum'] > 0.1) & (df['risk_score'] < 0.2),
            (df['momentum'] > 0.03) & (df['risk_score'] < 0.15),
            (df['momentum'] < -0.1) & (df['risk_score'] < 0.2),
            (df['momentum'] < -0.03) & (df['risk_score'] < 0.15)
        ]
        choices = ['STRONG_BUY', 'BUY', 'STRONG_SELL', 'SELL']
        df['trading_signal'] = 'HOLD'
        
        for condition, choice in zip(conditions, choices):
            df.loc[condition, 'trading_signal'] = choice
        
        logger.info("   ✅ Technical indicators calculated")
        return df
    
    def detect_anomalies(self, df):
        """
        Détection d'anomalies et patterns suspects (logique Spark)
        """
        logger.info("🔍 Detecting anomalies...")
        
        # Score d'anomalie basé sur les mouvements extrêmes
        df['anomaly_score'] = (
            abs(df['momentum']) * 5 +
            (df['volume_usd'] / df['volume_usd'].median()) * 2 +
            df['spread_percent'] * 3
        )
        
        # Niveaux d'anomalie
        df['anomaly_level'] = pd.cut(
            df['anomaly_score'],
            bins=[-float('inf'), 2, 5, 8, 12, float('inf')],
            labels=['NORMAL', 'LOW', 'MODERATE', 'HIGH', 'CRITICAL']
        )
        
        # Alertes critiques
        df['critical_alert'] = df['anomaly_level'].isin(['HIGH', 'CRITICAL'])
        
        # Recommandations finales
        df['recommendation'] = 'MONITOR'
        df.loc[df['anomaly_level'] == 'CRITICAL', 'recommendation'] = 'AVOID'
        df.loc[
            (df['trading_signal'].isin(['STRONG_BUY', 'BUY'])) & 
            (df['anomaly_level'].isin(['NORMAL', 'LOW'])), 
            'recommendation'
        ] = 'BUY_RECOMMENDED'
        df.loc[
            (df['trading_signal'].isin(['STRONG_SELL', 'SELL'])) & 
            (df['anomaly_level'].isin(['NORMAL', 'LOW'])), 
            'recommendation'
        ] = 'SELL_RECOMMENDED'
        df.loc[df['anomaly_level'].isin(['HIGH', 'MODERATE']), 'recommendation'] = 'CAUTION'
        
        logger.info("   ✅ Anomaly detection completed")
        return df
    
    def save_to_mongodb(self, df):
        """
        Sauvegarde vers MongoDB
        """
        logger.info("💾 Saving to MongoDB...")
        
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            
            # Nettoyer les anciennes données
            db.processed_data.delete_many({})
            db.critical_alerts.delete_many({})
            db.market_metrics.delete_many({})
            
            # Préparer les données
            df['processing_timestamp'] = datetime.now().isoformat()
            records = df.to_dict('records')
            
            # Insérer les données principales
            db.processed_data.insert_many(records)
            
            # Créer les alertes critiques
            critical_records = df[df['critical_alert']]
            if len(critical_records) > 0:
                alerts = []
                for _, row in critical_records.iterrows():
                    alert = {
                        "symbol": row['symbol'],
                        "alert_type": f"{row['anomaly_level']}_ANOMALY",
                        "alert_message": f"{row['symbol']}: {row['recommendation']} - {row['anomaly_level']} anomaly detected",
                        "anomaly_score": float(row['anomaly_score']),
                        "price": float(row['lastPrice']),
                        "change_percent": float(row['priceChangePercent']),
                        "volume_usd": float(row['volume_usd']),
                        "recommendation": row['recommendation'],
                        "timestamp": datetime.now().isoformat()
                    }
                    alerts.append(alert)
                
                db.critical_alerts.insert_many(alerts)
                logger.info(f"   🚨 {len(alerts)} critical alerts saved")
            
            # Créer résumé de marché
            summary = {
                "timestamp": datetime.now().isoformat(),
                "data_source": "binance_spark_analytics",
                "total_symbols": len(df),
                "total_volume_usd": float(df["volume_usd"].sum()),
                "average_price_change": float(df["priceChangePercent"].mean()),
                "risk_distribution": df["risk_category"].value_counts().to_dict(),
                "signal_distribution": df["trading_signal"].value_counts().to_dict(),
                "anomaly_distribution": df["anomaly_level"].value_counts().to_dict(),
                "recommendation_distribution": df["recommendation"].value_counts().to_dict(),
                "critical_alerts": int(df['critical_alert'].sum()),
                "top_gainers": df.nlargest(5, 'priceChangePercent')[['symbol', 'priceChangePercent']].to_dict('records'),
                "top_losers": df.nsmallest(5, 'priceChangePercent')[['symbol', 'priceChangePercent']].to_dict('records')
            }
            
            db.market_metrics.insert_one(summary)
            
            logger.info(f"   ✅ {len(records)} records saved to MongoDB")
            logger.info(f"   💰 Total volume: ${summary['total_volume_usd']:,.0f}")
            
            client.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ MongoDB save failed: {e}")
            return False
    
    def run_analysis(self):
        """
        Pipeline complet d'analyse
        """
        logger.info("🔥 BINANCE SPARK ANALYTICS")
        logger.info("=" * 50)
        
        try:
            # 1. Récupérer les données
            data = self.fetch_binance_data()
            if not data:
                return False
            
            # 2. Nettoyer et filtrer
            df_clean = self.clean_and_filter_data(data)
            
            # 3. Calculer les indicateurs
            df_indicators = self.calculate_technical_indicators(df_clean)
            
            # 4. Détecter les anomalies
            df_final = self.detect_anomalies(df_indicators)
            
            # 5. Afficher les résultats
            logger.info("\n📊 ANALYSIS RESULTS:")
            logger.info("-" * 30)
            logger.info(f"Total symbols analyzed: {len(df_final)}")
            logger.info(f"Critical alerts: {df_final['critical_alert'].sum()}")
            logger.info(f"Average price change: {df_final['priceChangePercent'].mean():.2f}%")
            logger.info(f"Total volume: ${df_final['volume_usd'].sum():,.0f}")
            
            # 6. Sauvegarder
            success = self.save_to_mongodb(df_final)
            
            if success:
                logger.info("\n" + "=" * 50)
                logger.info("🎉 ANALYSIS COMPLETED SUCCESSFULLY!")
                logger.info("=" * 50)
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"❌ Analysis failed: {e}")
            return False

def main():
    """
    Point d'entrée principal
    """
    import sys
    
    # Vérifier les arguments
    use_hdfs = "--hdfs" in sys.argv
    json_file = None
    
    # Chercher un argument --json-file
    for i, arg in enumerate(sys.argv):
        if arg == "--json-file" and i + 1 < len(sys.argv):
            json_file = sys.argv[i + 1]
            break
    
    if json_file:
        logger.info(f"🔥 Using JSON file mode: {json_file}")
        analyzer = BinanceSparkAnalytics(json_file=json_file)
    elif use_hdfs:
        logger.info("🔥 Using HDFS mode")
        analyzer = BinanceSparkAnalytics(use_hdfs=True)
    else:
        logger.info("🔥 Using API mode (default)")
        analyzer = BinanceSparkAnalytics(use_hdfs=False)
    
    success = analyzer.run_analysis()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())

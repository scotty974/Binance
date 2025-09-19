#!/usr/bin/env python3
"""
Pipeline ETL Spark Principal - VOTRE MISSION
HDFS → Spark (nettoyage + analyses + anomalies) → MongoDB (par batch)
"""

import sys
import os
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp
from pymongo import MongoClient
import pandas as pd
import logging

# Ajouter le chemin des jobs Spark
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'spark_jobs'))

from cleaning import BinanceDataCleaner
from indicators import TechnicalIndicators  
from anomalies import AnomalyDetector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceSparkETL:
    def __init__(self):
        """
        Pipeline ETL Spark complet : HDFS → Analyses → MongoDB
        """
        self.spark = SparkSession.builder \
            .appName("BinanceSparkETL") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Initialiser les composants Spark
        self.cleaner = BinanceDataCleaner(self.spark)
        self.indicators = TechnicalIndicators(self.spark)
        self.anomaly_detector = AnomalyDetector(self.spark)
        
        logger.info("🔥 Binance Spark ETL Pipeline initialized")
    
    def load_from_hdfs(self, hdfs_path):
        """
        Charge les données depuis HDFS
        """
        logger.info(f"📥 Loading from HDFS: {hdfs_path}")
        
        try:
            # Lire depuis HDFS
            df = self.spark.read.option("multiline", "true").json(hdfs_path)
            
            count = df.count()
            logger.info(f"   📊 {count} records loaded from HDFS")
            
            if count > 0:
                logger.info("   🔍 Data preview:")
                df.select("symbol", "lastPrice", "priceChangePercent", "volume").show(5)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error loading from HDFS: {e}")
            
            # Fallback: essayer local
            try:
                logger.info("🔄 Trying local fallback...")
                local_path = hdfs_path.replace("hdfs://namenode:9000", ".")
                if not local_path.startswith("./"):
                    local_path = "./" + local_path.lstrip("/")
                
                df = self.spark.read.option("multiline", "true").json(local_path)
                logger.info(f"   📊 {df.count()} records loaded from local")
                return df
                
            except Exception as e2:
                logger.error(f"❌ Local fallback failed: {e2}")
                raise
    
    def apply_spark_processing(self, df):
        """
        Applique tous les traitements Spark (VOTRE PARTIE PRINCIPALE)
        """
        logger.info("🔥 APPLYING SPARK PROCESSING")
        logger.info("=" * 50)
        
        try:
            # 1. Nettoyage des données
            logger.info("🧹 Step 1: Data cleaning...")
            cleaned_df = self.cleaner.clean_binance_data(df)
            logger.info(f"   ✅ {cleaned_df.count()} records cleaned")
            
            # 2. Calcul des indicateurs techniques
            logger.info("📈 Step 2: Technical indicators...")
            indicators_df = self.indicators.calculate_all_indicators(cleaned_df)
            logger.info(f"   ✅ Indicators calculated")
            
            # 3. Détection d'anomalies
            logger.info("🔍 Step 3: Anomaly detection...")
            final_df = self.anomaly_detector.detect_all_anomalies(indicators_df)
            logger.info(f"   ✅ Anomalies detected")
            
            # 4. Aperçu des résultats
            logger.info("\n📊 SPARK PROCESSING RESULTS:")
            logger.info("-" * 40)
            
            final_count = final_df.count()
            logger.info(f"   📈 Total processed: {final_count} symbols")
            
            # Statistiques
            risk_stats = final_df.groupBy("risk_category").count().collect()
            signal_stats = final_df.groupBy("trading_signal").count().collect()
            anomaly_stats = final_df.groupBy("anomaly_level").count().collect()
            
            logger.info("   ⚖️ Risk distribution:")
            for row in risk_stats:
                logger.info(f"      {row['risk_category']}: {row['count']}")
            
            logger.info("   🎯 Signal distribution:")
            for row in signal_stats:
                logger.info(f"      {row['trading_signal']}: {row['count']}")
            
            logger.info("   🚨 Anomaly distribution:")
            for row in anomaly_stats:
                logger.info(f"      {row['anomaly_level']}: {row['count']}")
            
            return final_df
            
        except Exception as e:
            logger.error(f"❌ Spark processing failed: {e}")
            raise
    
    def save_to_mongodb_batch(self, df, batch_size=100):
        """
        Sauvegarde par batch vers MongoDB
        """
        logger.info(f"💾 Saving to MongoDB (batch size: {batch_size})...")
        
        try:
            # Connexion MongoDB (port interne du container)
            client = MongoClient("mongodb://db:27017/")
            db = client.binance_analytics
            
            # Collections
            processed_collection = db.processed_data
            alerts_collection = db.critical_alerts
            metrics_collection = db.market_metrics
            
            # Convertir en Pandas pour traitement par batch
            pandas_df = df.toPandas()
            total_records = len(pandas_df)
            
            logger.info(f"   📊 Processing {total_records} records in batches...")
            
            # Sauvegarde par batch
            saved_count = 0
            alerts_count = 0
            
            for i in range(0, total_records, batch_size):
                batch_df = pandas_df.iloc[i:i+batch_size]
                batch_records = batch_df.to_dict('records')
                
                # Insérer le batch principal
                processed_collection.insert_many(batch_records)
                saved_count += len(batch_records)
                
                # Extraire les alertes critiques
                critical_batch = batch_df[batch_df['critical_alert'] == True]
                
                for _, row in critical_batch.iterrows():
                    alert_record = {
                        "symbol": row['symbol'],
                        "alert_type": f"{row['anomaly_level']}_ANOMALY",
                        "alert_message": f"{row['symbol']}: {row['recommendation']} - {row['anomaly_level']} anomaly",
                        "anomaly_score": row['overall_anomaly_score'],
                        "price": row['last_price'],
                        "change_percent": row['price_change_percent'],
                        "volume_usd": row['volume_usd'],
                        "pump_dump_risk": row['pump_dump_risk'],
                        "recommendation": row['recommendation'],
                        "timestamp": datetime.now().isoformat()
                    }
                    alerts_collection.insert_one(alert_record)
                    alerts_count += 1
                
                logger.info(f"   📦 Batch {i//batch_size + 1}: {len(batch_records)} records saved")
            
            # Créer résumé de marché
            market_summary = {
                "timestamp": datetime.now().isoformat(),
                "data_source": "spark_etl_pipeline",
                "total_symbols": total_records,
                "total_volume_usd": float(pandas_df["volume_usd"].sum()),
                "average_price_change": float(pandas_df["price_change_percent"].mean()),
                "risk_distribution": pandas_df["risk_category"].value_counts().to_dict(),
                "signal_distribution": pandas_df["trading_signal"].value_counts().to_dict(),
                "anomaly_distribution": pandas_df["anomaly_level"].value_counts().to_dict(),
                "recommendation_distribution": pandas_df["recommendation"].value_counts().to_dict(),
                "critical_alerts": alerts_count,
                "top_gainers": pandas_df.nlargest(5, 'price_change_percent')[['symbol', 'price_change_percent']].to_dict('records'),
                "top_losers": pandas_df.nsmallest(5, 'price_change_percent')[['symbol', 'price_change_percent']].to_dict('records'),
                "highest_volume": pandas_df.nlargest(5, 'volume_usd')[['symbol', 'volume_usd']].to_dict('records'),
                "most_anomalous": pandas_df.nlargest(5, 'overall_anomaly_score')[['symbol', 'overall_anomaly_score', 'anomaly_level']].to_dict('records')
            }
            
            metrics_collection.insert_one(market_summary)
            
            logger.info(f"   ✅ Saved: {saved_count} records, {alerts_count} alerts")
            logger.info(f"   💰 Total volume: ${market_summary['total_volume_usd']:,.0f}")
            
            client.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ MongoDB save failed: {e}")
            return False
    
    def run_etl_pipeline(self, hdfs_path, batch_size=100):
        """
        Pipeline ETL complet : HDFS → Spark → MongoDB
        """
        logger.info("🚀 STARTING BINANCE SPARK ETL PIPELINE")
        logger.info("=" * 60)
        logger.info("HDFS → Spark (Clean + Indicators + Anomalies) → MongoDB")
        logger.info("=" * 60)
        
        try:
            # 1. Charger depuis HDFS
            raw_df = self.load_from_hdfs(hdfs_path)
            if raw_df.count() == 0:
                logger.error("❌ No data loaded from HDFS")
                return False
            
            # 2. Appliquer les traitements Spark
            processed_df = self.apply_spark_processing(raw_df)
            
            # 3. Sauvegarder vers MongoDB par batch
            success = self.save_to_mongodb_batch(processed_df, batch_size)
            
            if success:
                logger.info("\n" + "=" * 60)
                logger.info("🎉 SPARK ETL PIPELINE COMPLETED SUCCESSFULLY!")
                logger.info("=" * 60)
                logger.info("✅ HDFS → Data loaded")
                logger.info("✅ Spark → Data cleaned, analyzed, anomalies detected")
                logger.info("✅ MongoDB → Results saved by batch")
                logger.info("\n🌐 Check results in MongoDB: binance_analytics database")
                return True
            else:
                logger.error("❌ Failed to save to MongoDB")
                return False
                
        except Exception as e:
            logger.error(f"❌ ETL Pipeline failed: {e}")
            return False
        
        finally:
            self.spark.stop()

def main():
    """
    Point d'entrée principal
    """
    parser = argparse.ArgumentParser(description="Binance Spark ETL Pipeline")
    parser.add_argument("--hdfs-path", required=True, help="HDFS path to process")
    parser.add_argument("--batch-size", type=int, default=100, help="MongoDB batch size")
    
    args = parser.parse_args()
    
    pipeline = BinanceSparkETL()
    success = pipeline.run_etl_pipeline(args.hdfs_path, args.batch_size)
    
    if success:
        print("\n🎯 Spark ETL Pipeline completed successfully!")
        return 0
    else:
        print("\n❌ Spark ETL Pipeline failed!")
        return 1

if __name__ == "__main__":
    exit(main())

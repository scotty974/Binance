#!/usr/bin/env python3
"""
Job Spark - Détection d'anomalies et patterns suspects
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, abs as spark_abs, log, sqrt
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyDetector:
    def __init__(self, spark_session=None):
        """
        Détecteur d'anomalies avec Spark
        """
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = SparkSession.builder \
                .appName("AnomalyDetector") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
        
        logger.info("🔍 Anomaly Detector initialized")
    
    def detect_all_anomalies(self, df):
        """
        Détecte toutes les anomalies et patterns suspects
        """
        logger.info("🔍 Detecting anomalies and suspicious patterns...")
        
        try:
            # 1. Anomalies de prix (mouvements extrêmes)
            anomalies_df = df.withColumn(
                "price_anomaly_score",
                when(spark_abs(col("momentum")) > 0.2, 10.0)
                .when(spark_abs(col("momentum")) > 0.15, 8.0)
                .when(spark_abs(col("momentum")) > 0.1, 6.0)
                .when(spark_abs(col("momentum")) > 0.05, 4.0)
                .otherwise(1.0)
            )
            
            # 2. Anomalies de volume
            anomalies_df = anomalies_df.withColumn(
                "volume_anomaly_score",
                when(col("volume_usd") > 1000000000, 9.0)  # > 1B USD
                .when(col("volume_usd") > 500000000, 7.0)   # > 500M USD
                .when(col("volume_usd") > 100000000, 5.0)   # > 100M USD
                .when(col("volume_usd") > 10000000, 3.0)    # > 10M USD
                .when(col("volume_usd") < 50000, 6.0)       # < 50K USD (suspect)
                .otherwise(1.0)
            )
            
            # 3. Anomalies de spread (manipulation potentielle)
            anomalies_df = anomalies_df.withColumn(
                "spread_anomaly_score",
                when(col("spread_percent") > 5.0, 8.0)     # Spread très élevé
                .when(col("spread_percent") > 2.0, 6.0)    # Spread élevé
                .when(col("spread_percent") > 1.0, 4.0)    # Spread modéré
                .when(col("spread_percent") < 0.01, 7.0)   # Spread anormalement bas
                .otherwise(1.0)
            )
            
            # 4. Score d'anomalie global
            anomalies_df = anomalies_df.withColumn(
                "overall_anomaly_score",
                (col("price_anomaly_score") * 0.4 + 
                 col("volume_anomaly_score") * 0.35 + 
                 col("spread_anomaly_score") * 0.25)
            )
            
            # 5. Niveau d'anomalie
            anomalies_df = anomalies_df.withColumn(
                "anomaly_level",
                when(col("overall_anomaly_score") > 8.0, "CRITICAL")
                .when(col("overall_anomaly_score") > 6.0, "HIGH")
                .when(col("overall_anomaly_score") > 4.0, "MODERATE")
                .when(col("overall_anomaly_score") > 2.0, "LOW")
                .otherwise("NORMAL")
            )
            
            # 6. Détection de patterns de manipulation
            anomalies_df = anomalies_df.withColumn(
                "pump_dump_risk",
                when((spark_abs(col("momentum")) > 0.2) & (col("volume_anomaly_score") > 5), "VERY_HIGH")
                .when((spark_abs(col("momentum")) > 0.15) & (col("volume_anomaly_score") > 3), "HIGH")
                .when((spark_abs(col("momentum")) > 0.1) & (col("volume_anomaly_score") > 2), "MEDIUM")
                .when(spark_abs(col("momentum")) > 0.05, "LOW")
                .otherwise("MINIMAL")
            )
            
            # 7. Opportunités d'arbitrage
            anomalies_df = anomalies_df.withColumn(
                "arbitrage_opportunity",
                when(col("spread_percent") > 3.0, "EXCELLENT")
                .when(col("spread_percent") > 1.5, "GOOD")
                .when(col("spread_percent") > 0.8, "FAIR")
                .when(col("spread_percent") > 0.3, "POOR")
                .otherwise("NONE")
            )
            
            # 8. Détection de wash trading (volume suspect vs prix)
            anomalies_df = anomalies_df.withColumn(
                "wash_trading_risk",
                when((col("volume_anomaly_score") > 7) & (col("price_anomaly_score") < 3), "HIGH")
                .when((col("volume_anomaly_score") > 5) & (col("price_anomaly_score") < 4), "MEDIUM")
                .when((col("volume_anomaly_score") > 3) & (col("price_anomaly_score") < 2), "LOW")
                .otherwise("MINIMAL")
            )
            
            # 9. Recommandation finale basée sur les anomalies
            anomalies_df = anomalies_df.withColumn(
                "recommendation",
                when(col("anomaly_level") == "CRITICAL", "AVOID")
                .when(col("pump_dump_risk").isin(["VERY_HIGH", "HIGH"]), "EXTREME_CAUTION")
                .when(col("wash_trading_risk") == "HIGH", "INVESTIGATE")
                .when((col("trading_signal").isin(["STRONG_BUY", "BUY"])) & 
                      (col("anomaly_level").isin(["NORMAL", "LOW"])), "BUY_RECOMMENDED")
                .when((col("trading_signal").isin(["STRONG_SELL", "SELL"])) & 
                      (col("anomaly_level").isin(["NORMAL", "LOW"])), "SELL_RECOMMENDED")
                .when(col("arbitrage_opportunity").isin(["EXCELLENT", "GOOD"]), "ARBITRAGE_OPPORTUNITY")
                .when(col("anomaly_level").isin(["HIGH", "MODERATE"]), "CAUTION")
                .otherwise("MONITOR")
            )
            
            # 10. Alertes critiques
            anomalies_df = anomalies_df.withColumn(
                "critical_alert",
                when((col("anomaly_level") == "CRITICAL") | 
                     (col("pump_dump_risk") == "VERY_HIGH") |
                     (col("wash_trading_risk") == "HIGH"), True)
                .otherwise(False)
            )
            
            count = anomalies_df.count()
            critical_count = anomalies_df.filter(col("critical_alert") == True).count()
            
            logger.info(f"   📊 Anomalies analyzed for {count} symbols")
            logger.info(f"   🚨 Critical alerts: {critical_count}")
            logger.info("   ✅ Anomaly detection completed")
            
            return anomalies_df
            
        except Exception as e:
            logger.error(f"❌ Anomaly detection failed: {e}")
            raise

def main():
    """
    Test de détection d'anomalies
    """
    detector = AnomalyDetector()
    
    # Test avec des données
    test_data = [
        {"symbol": "TESTUSDT", "momentum": 0.25, "volume_usd": 2000000000.0, 
         "spread_percent": 0.1, "trading_signal": "STRONG_BUY"}
    ]
    
    test_df = detector.spark.createDataFrame(test_data)
    anomalies_df = detector.detect_all_anomalies(test_df)
    
    print("🔍 Anomaly detection test completed:")
    anomalies_df.select("symbol", "anomaly_level", "pump_dump_risk", "recommendation").show()
    
    detector.spark.stop()

if __name__ == "__main__":
    main()

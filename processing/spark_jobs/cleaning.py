#!/usr/bin/env python3
"""
Job Spark - Nettoyage des données Binance depuis HDFS
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, regexp_replace
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceDataCleaner:
    def __init__(self, spark_session=None):
        """
        Nettoyeur de données Binance avec Spark
        """
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = SparkSession.builder \
                .appName("BinanceDataCleaner") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
        
        logger.info("🧹 Binance Data Cleaner initialized")
    
    def clean_binance_data(self, df):
        """
        Nettoie et valide les données Binance
        """
        logger.info("🧹 Starting data cleaning...")
        
        try:
            # 1. Sélectionner et convertir les colonnes importantes
            cleaned_df = df.select(
                col("symbol"),
                col("lastPrice").cast("double").alias("last_price"),
                col("priceChangePercent").cast("double").alias("price_change_percent"),
                col("volume").cast("double").alias("volume"),
                col("bidPrice").cast("double").alias("bid_price"),
                col("askPrice").cast("double").alias("ask_price"),
                col("openPrice").cast("double").alias("open_price"),
                col("highPrice").cast("double").alias("high_price"),
                col("lowPrice").cast("double").alias("low_price"),
                col("quoteVolume").cast("double").alias("quote_volume"),
                current_timestamp().alias("processing_timestamp")
            )
            
            # 2. Filtrer les données invalides
            validated_df = cleaned_df.filter(
                (col("last_price") > 0) & 
                (col("volume") > 0) &
                (col("bid_price") > 0) &
                (col("ask_price") > col("bid_price")) &
                (col("open_price") > 0) &
                (col("high_price") >= col("low_price"))
            )
            
            # 3. Ajouter des colonnes calculées
            enriched_df = validated_df.withColumn(
                "spread", col("ask_price") - col("bid_price")
            ).withColumn(
                "spread_percent", (col("spread") / col("last_price")) * 100
            ).withColumn(
                "price_range", col("high_price") - col("low_price")
            ).withColumn(
                "volatility", col("price_range") / col("open_price")
            ).withColumn(
                "volume_usd", col("volume") * col("last_price")
            ).withColumn(
                "data_quality", 
                when((col("spread_percent") < 5) & (col("volatility") < 1), "HIGH")
                .when((col("spread_percent") < 10) & (col("volatility") < 2), "MEDIUM")
                .otherwise("LOW")
            )
            
            # 4. Filtrer seulement les paires USDT de qualité
            final_df = enriched_df.filter(
                col("symbol").endswith("USDT") &
                (col("data_quality").isin(["HIGH", "MEDIUM"])) &
                (col("volume_usd") > 10000)  # Volume minimum
            )
            
            initial_count = df.count()
            final_count = final_df.count()
            
            logger.info(f"   📊 Cleaned: {initial_count} → {final_count} records")
            logger.info(f"   ✅ Data cleaning completed")
            
            return final_df
            
        except Exception as e:
            logger.error(f"❌ Cleaning failed: {e}")
            raise

def main():
    """
    Test du nettoyage
    """
    cleaner = BinanceDataCleaner()
    
    # Test avec des données locales
    test_data = [
        {"symbol": "BTCUSDT", "lastPrice": "45000", "priceChangePercent": "2.5", 
         "volume": "1000", "bidPrice": "44999", "askPrice": "45001",
         "openPrice": "44000", "highPrice": "45500", "lowPrice": "43500", "quoteVolume": "45000000"}
    ]
    
    test_df = cleaner.spark.createDataFrame(test_data)
    cleaned_df = cleaner.clean_binance_data(test_df)
    
    print("🧹 Cleaning test completed:")
    cleaned_df.show()
    
    cleaner.spark.stop()

if __name__ == "__main__":
    main()

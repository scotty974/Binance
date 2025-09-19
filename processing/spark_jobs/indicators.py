#!/usr/bin/env python3
"""
Job Spark - Calcul d'indicateurs techniques
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, abs as spark_abs, sqrt, log
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TechnicalIndicators:
    def __init__(self, spark_session=None):
        """
        Calculateur d'indicateurs techniques avec Spark
        """
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = SparkSession.builder \
                .appName("TechnicalIndicators") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
        
        logger.info("📈 Technical Indicators calculator initialized")
    
    def calculate_all_indicators(self, df):
        """
        Calcule tous les indicateurs techniques
        """
        logger.info("📈 Calculating technical indicators...")
        
        try:
            # 1. Indicateurs de base
            indicators_df = df.withColumn(
                "momentum", col("price_change_percent") / 100
            ).withColumn(
                "liquidity_score", 1 / (col("spread_percent") + 0.001)
            ).withColumn(
                "volatility_score", col("volatility") * 100
            )
            
            # 2. Score de force du signal
            indicators_df = indicators_df.withColumn(
                "signal_strength", 
                when(col("momentum") > 0, col("momentum") * 1000)
                .otherwise(spark_abs(col("momentum")) * 1000)
            )
            
            # 3. Indicateur de tendance
            indicators_df = indicators_df.withColumn(
                "trend_direction",
                when(col("last_price") > col("open_price"), "BULLISH")
                .when(col("last_price") < col("open_price"), "BEARISH")
                .otherwise("NEUTRAL")
            )
            
            # 4. Score de momentum avancé
            indicators_df = indicators_df.withColumn(
                "momentum_category",
                when(col("momentum") > 0.15, "VERY_STRONG_UP")
                .when(col("momentum") > 0.05, "STRONG_UP")
                .when(col("momentum") > 0.01, "WEAK_UP")
                .when(col("momentum") > -0.01, "NEUTRAL")
                .when(col("momentum") > -0.05, "WEAK_DOWN")
                .when(col("momentum") > -0.15, "STRONG_DOWN")
                .otherwise("VERY_STRONG_DOWN")
            )
            
            # 5. Indicateur de liquidité
            indicators_df = indicators_df.withColumn(
                "liquidity_category",
                when(col("liquidity_score") > 100, "VERY_HIGH")
                .when(col("liquidity_score") > 50, "HIGH")
                .when(col("liquidity_score") > 20, "MEDIUM")
                .when(col("liquidity_score") > 5, "LOW")
                .otherwise("VERY_LOW")
            )
            
            # 6. Score de risque composite
            indicators_df = indicators_df.withColumn(
                "risk_score",
                (col("volatility_score") * 0.4 + 
                 (100 - col("liquidity_score")) * 0.3 + 
                 spark_abs(col("momentum")) * 100 * 0.3) / 100
            ).withColumn(
                "risk_category",
                when(col("risk_score") < 0.3, "LOW")
                .when(col("risk_score") < 0.6, "MEDIUM")
                .when(col("risk_score") < 0.8, "HIGH")
                .otherwise("VERY_HIGH")
            )
            
            # 7. Signaux de trading
            indicators_df = indicators_df.withColumn(
                "trading_signal",
                when((col("momentum") > 0.1) & (col("risk_score") < 0.7), "STRONG_BUY")
                .when((col("momentum") > 0.03) & (col("risk_score") < 0.5), "BUY")
                .when((col("momentum") < -0.1) & (col("risk_score") < 0.7), "STRONG_SELL")
                .when((col("momentum") < -0.03) & (col("risk_score") < 0.5), "SELL")
                .otherwise("HOLD")
            )
            
            # 8. Score de confiance du signal
            indicators_df = indicators_df.withColumn(
                "signal_confidence",
                when(col("trading_signal").isin(["STRONG_BUY", "STRONG_SELL"]), 
                     col("signal_strength") * (1 - col("risk_score")))
                .when(col("trading_signal").isin(["BUY", "SELL"]), 
                     col("signal_strength") * 0.7 * (1 - col("risk_score")))
                .otherwise(col("signal_strength") * 0.3)
            )
            
            count = indicators_df.count()
            logger.info(f"   📊 Indicators calculated for {count} symbols")
            logger.info("   ✅ Technical indicators completed")
            
            return indicators_df
            
        except Exception as e:
            logger.error(f"❌ Indicators calculation failed: {e}")
            raise

def main():
    """
    Test des indicateurs
    """
    calculator = TechnicalIndicators()
    
    # Test avec des données
    test_data = [
        {"symbol": "BTCUSDT", "last_price": 45000.0, "price_change_percent": 2.5, 
         "volume": 1000.0, "spread_percent": 0.1, "volatility": 0.05, "open_price": 44000.0}
    ]
    
    test_df = calculator.spark.createDataFrame(test_data)
    indicators_df = calculator.calculate_all_indicators(test_df)
    
    print("📈 Indicators test completed:")
    indicators_df.select("symbol", "momentum", "risk_category", "trading_signal").show()
    
    calculator.spark.stop()

if __name__ == "__main__":
    main()

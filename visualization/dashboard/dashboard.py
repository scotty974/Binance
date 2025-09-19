#!/usr/bin/env python3
"""
Dashboard de visualisation des résultats Spark
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pymongo import MongoClient
from datetime import datetime, timedelta
import numpy as np

# Configuration de la page
st.set_page_config(
    page_title="Binance Spark Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

class BinanceDashboard:
    def __init__(self):
        """
        Dashboard pour visualiser les résultats Spark
        """
        self.mongo_client = MongoClient("mongodb://localhost:27018/")  # Port externe modifié
        self.db = self.mongo_client.binance_analytics
    
    def load_data(self):
        """
        Charge les données depuis MongoDB
        """
        try:
            # Données principales
            processed_data = list(self.db.processed_data.find().sort("processing_timestamp", -1).limit(1000))
            processed_df = pd.DataFrame(processed_data) if processed_data else pd.DataFrame()
            
            # Alertes critiques
            alerts_data = list(self.db.critical_alerts.find().sort("timestamp", -1).limit(100))
            alerts_df = pd.DataFrame(alerts_data) if alerts_data else pd.DataFrame()
            
            # Métriques de marché
            metrics_data = list(self.db.market_metrics.find().sort("timestamp", -1).limit(10))
            metrics_df = pd.DataFrame(metrics_data) if metrics_data else pd.DataFrame()
            
            return processed_df, alerts_df, metrics_df
            
        except Exception as e:
            st.error(f"Erreur de connexion MongoDB: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    def render_header(self):
        """
        Affiche l'en-tête du dashboard
        """
        st.title("🔥 Binance Spark Analytics Dashboard")
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("🎯 Mission", "Spark ETL", "HDFS → MongoDB")
        with col2:
            st.metric("⚡ Engine", "Apache Spark", "Distributed Processing")
        with col3:
            st.metric("🕒 Last Update", datetime.now().strftime("%H:%M:%S"))
    
    def render_overview(self, processed_df, alerts_df, metrics_df):
        """
        Vue d'ensemble des données
        """
        st.header("📊 Vue d'Ensemble")
        
        if not processed_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("💰 Cryptos Analysées", len(processed_df), 
                         delta=f"+{len(processed_df)} nouvelles")
            
            with col2:
                total_volume = processed_df['volume_usd'].sum() if 'volume_usd' in processed_df.columns else 0
                st.metric("💵 Volume Total", f"${total_volume:,.0f}", 
                         delta="Volume 24h")
            
            with col3:
                critical_alerts = len(alerts_df) if not alerts_df.empty else 0
                st.metric("🚨 Alertes Critiques", critical_alerts, 
                         delta="Anomalies détectées")
            
            with col4:
                avg_change = processed_df['price_change_percent'].mean() if 'price_change_percent' in processed_df.columns else 0
                st.metric("📈 Variation Moyenne", f"{avg_change:.2f}%", 
                         delta="24h")
        else:
            st.warning("Aucune donnée disponible. Lancez le pipeline Spark ETL.")
    
    def render_risk_analysis(self, processed_df):
        """
        Analyse des risques
        """
        st.header("⚖️ Analyse des Risques")
        
        if not processed_df.empty and 'risk_category' in processed_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribution des risques
                risk_counts = processed_df['risk_category'].value_counts()
                fig_risk = px.pie(
                    values=risk_counts.values, 
                    names=risk_counts.index,
                    title="Distribution des Catégories de Risque",
                    color_discrete_map={
                        'LOW': '#00ff00',
                        'MEDIUM': '#ffff00', 
                        'HIGH': '#ff8000',
                        'VERY_HIGH': '#ff0000'
                    }
                )
                st.plotly_chart(fig_risk, use_container_width=True)
            
            with col2:
                # Top cryptos par risque
                if 'overall_anomaly_score' in processed_df.columns:
                    top_risk = processed_df.nlargest(10, 'overall_anomaly_score')
                    fig_top_risk = px.bar(
                        top_risk, 
                        x='symbol', 
                        y='overall_anomaly_score',
                        title="Top 10 - Scores d'Anomalie",
                        color='risk_category',
                        color_discrete_map={
                            'LOW': '#00ff00',
                            'MEDIUM': '#ffff00', 
                            'HIGH': '#ff8000',
                            'VERY_HIGH': '#ff0000'
                        }
                    )
                    st.plotly_chart(fig_top_risk, use_container_width=True)
    
    def render_trading_signals(self, processed_df):
        """
        Signaux de trading
        """
        st.header("🎯 Signaux de Trading")
        
        if not processed_df.empty and 'trading_signal' in processed_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribution des signaux
                signal_counts = processed_df['trading_signal'].value_counts()
                fig_signals = px.bar(
                    x=signal_counts.index, 
                    y=signal_counts.values,
                    title="Distribution des Signaux de Trading",
                    color=signal_counts.index,
                    color_discrete_map={
                        'STRONG_BUY': '#00ff00',
                        'BUY': '#80ff80',
                        'HOLD': '#ffff00',
                        'SELL': '#ff8080',
                        'STRONG_SELL': '#ff0000'
                    }
                )
                st.plotly_chart(fig_signals, use_container_width=True)
            
            with col2:
                # Recommandations
                if 'recommendation' in processed_df.columns:
                    rec_counts = processed_df['recommendation'].value_counts()
                    fig_rec = px.pie(
                        values=rec_counts.values, 
                        names=rec_counts.index,
                        title="Recommandations Finales"
                    )
                    st.plotly_chart(fig_rec, use_container_width=True)
    
    def render_anomalies(self, processed_df, alerts_df):
        """
        Détection d'anomalies
        """
        st.header("🔍 Détection d'Anomalies")
        
        if not processed_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Niveaux d'anomalies
                if 'anomaly_level' in processed_df.columns:
                    anomaly_counts = processed_df['anomaly_level'].value_counts()
                    fig_anomaly = px.bar(
                        x=anomaly_counts.index, 
                        y=anomaly_counts.values,
                        title="Niveaux d'Anomalies Détectées",
                        color=anomaly_counts.index,
                        color_discrete_map={
                            'NORMAL': '#00ff00',
                            'LOW': '#80ff80',
                            'MODERATE': '#ffff00',
                            'HIGH': '#ff8000',
                            'CRITICAL': '#ff0000'
                        }
                    )
                    st.plotly_chart(fig_anomaly, use_container_width=True)
            
            with col2:
                # Alertes critiques récentes
                if not alerts_df.empty:
                    st.subheader("🚨 Alertes Critiques Récentes")
                    for _, alert in alerts_df.head(5).iterrows():
                        st.error(f"**{alert['symbol']}**: {alert['alert_message']}")
    
    def render_performance(self, processed_df):
        """
        Performance du marché
        """
        st.header("📈 Performance du Marché")
        
        if not processed_df.empty and 'price_change_percent' in processed_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top gainers
                top_gainers = processed_df.nlargest(10, 'price_change_percent')
                fig_gainers = px.bar(
                    top_gainers, 
                    x='symbol', 
                    y='price_change_percent',
                    title="🚀 Top 10 Gainers (24h)",
                    color='price_change_percent',
                    color_continuous_scale='Greens'
                )
                st.plotly_chart(fig_gainers, use_container_width=True)
            
            with col2:
                # Top losers
                top_losers = processed_df.nsmallest(10, 'price_change_percent')
                fig_losers = px.bar(
                    top_losers, 
                    x='symbol', 
                    y='price_change_percent',
                    title="📉 Top 10 Losers (24h)",
                    color='price_change_percent',
                    color_continuous_scale='Reds'
                )
                st.plotly_chart(fig_losers, use_container_width=True)
    
    def render_data_table(self, processed_df):
        """
        Table des données détaillées
        """
        st.header("📋 Données Détaillées")
        
        if not processed_df.empty:
            # Filtres
            col1, col2, col3 = st.columns(3)
            
            with col1:
                risk_filter = st.selectbox(
                    "Filtrer par Risque",
                    ['Tous'] + list(processed_df['risk_category'].unique()) if 'risk_category' in processed_df.columns else ['Tous']
                )
            
            with col2:
                signal_filter = st.selectbox(
                    "Filtrer par Signal",
                    ['Tous'] + list(processed_df['trading_signal'].unique()) if 'trading_signal' in processed_df.columns else ['Tous']
                )
            
            with col3:
                anomaly_filter = st.selectbox(
                    "Filtrer par Anomalie",
                    ['Tous'] + list(processed_df['anomaly_level'].unique()) if 'anomaly_level' in processed_df.columns else ['Tous']
                )
            
            # Appliquer les filtres
            filtered_df = processed_df.copy()
            
            if risk_filter != 'Tous' and 'risk_category' in processed_df.columns:
                filtered_df = filtered_df[filtered_df['risk_category'] == risk_filter]
            
            if signal_filter != 'Tous' and 'trading_signal' in processed_df.columns:
                filtered_df = filtered_df[filtered_df['trading_signal'] == signal_filter]
            
            if anomaly_filter != 'Tous' and 'anomaly_level' in processed_df.columns:
                filtered_df = filtered_df[filtered_df['anomaly_level'] == anomaly_filter]
            
            # Colonnes à afficher
            display_columns = ['symbol', 'last_price', 'price_change_percent', 'volume_usd', 
                             'risk_category', 'trading_signal', 'anomaly_level', 'recommendation']
            
            available_columns = [col for col in display_columns if col in filtered_df.columns]
            
            if available_columns:
                st.dataframe(
                    filtered_df[available_columns].head(100),
                    use_container_width=True
                )
            else:
                st.warning("Colonnes de données non disponibles")
    
    def run(self):
        """
        Lance le dashboard
        """
        # Charger les données
        processed_df, alerts_df, metrics_df = self.load_data()
        
        # Sidebar
        st.sidebar.title("🔥 Spark Analytics")
        st.sidebar.markdown("---")
        
        refresh_button = st.sidebar.button("🔄 Actualiser les Données")
        if refresh_button:
            st.rerun()
        
        st.sidebar.markdown("### 📊 Statistiques")
        if not processed_df.empty:
            st.sidebar.metric("Cryptos", len(processed_df))
            st.sidebar.metric("Alertes", len(alerts_df))
            st.sidebar.metric("Métriques", len(metrics_df))
        
        # Contenu principal
        self.render_header()
        self.render_overview(processed_df, alerts_df, metrics_df)
        self.render_risk_analysis(processed_df)
        self.render_trading_signals(processed_df)
        self.render_anomalies(processed_df, alerts_df)
        self.render_performance(processed_df)
        self.render_data_table(processed_df)

def main():
    """
    Point d'entrée principal
    """
    dashboard = BinanceDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()
Projet_Hadoop_Spark_Binance/
│
├── data/                        # Données brutes et préparées
│   ├── raw/                     # Données brutes (API Binance)
│   ├── processed/               # Données nettoyées pour Spark
│   └── exports/                 # Résultats pour MongoDB / visualisation
│
├── ingestion/                   # Scripts d’ingestion
│   ├── fetch_binance.py         # Récupère les données depuis Binance API
│   ├── hdfs_upload.sh           # Script pour envoyer dans Hadoop (HDFS)
│   └── mongo_insert.py          # Insère données nettoyées dans MongoDB
│
├── processing/                  # Traitement et analyse Spark
│   ├── spark_jobs/              # Jobs Spark organisés
│   │   ├── cleaning.py          # Nettoyage / formatage
│   │   ├── indicators.py        # Calcul d’indicateurs (moyenne, volatilité…)
│   │   └── anomalies.py         # Détection de patterns/anomalies
│   └── pipelines/               # Workflow complet Spark
│       └── pipeline_etl.py      # Pipeline ETL principal
│
├── visualization/               # Visualisation et présentation
│   ├── dashboards/              # Dashboards (Dash, Flask, Streamlit…)
│   │   └── dashboard.py
│   ├── notebooks/               # Jupyter Notebooks pour graphiques rapides
│   └── exports/                 # Graphiques ou CSV finaux
│
├── config/                      # Configurations
│   ├── hadoop_conf.yaml         # Connexion Hadoop
│   ├── mongo_conf.yaml          # Connexion MongoDB
│   └── spark_conf.yaml          # Config Spark
│
├── docs/                        # Documentation
│   ├── rapport.md               # Rapport technique
│   ├── archi.png                # Schéma architecture
│   └── readme.md
│
├── tests/                       # Tests unitaires
│   ├── test_ingestion.py
│   ├── test_processing.py
│   └── test_visualization.py
│
├── requirements.txt             # Dépendances Python
├── docker-compose.yml           # (optionnel) Lancer Hadoop + Mongo en containers
└── README.md                    # Guide d’installation et d’utilisation

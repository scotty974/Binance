from pymongo import MongoClient

# --- Configuration MongoDB ---
MONGO_URI = "mongodb://db:27017"  # Port interne du container (reste 27017)
MONGO_DB = "binance_db"
MONGO_COLLECTION = "ticker_24h"

def insert_into_mongo(data):
    """
    Insère les données Binance dans MongoDB.
    
    Args:
        data (list or dict): Liste ou dictionnaire de données à insérer.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        if isinstance(data, list):
            collection.insert_many(data)
        else:
            collection.insert_one(data)

        print("[INFO] Données insérées dans MongoDB")
    except Exception as e:
        print(f"[ERROR] Impossible d'insérer dans MongoDB: {e}")

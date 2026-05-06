import json
import psycopg2
import time
from kafka import KafkaConsumer

# Configuration des paramètres de connexion à la base de données PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "ecommerce_db",
    "user": "user",
    "password": "password",
    "port": 5432
}

def setup_database():
    """
    Initialise la table 'orders' dans PostgreSQL si elle n'existe pas encore.
    """
    print("⏳ Tentative de connexion à Postgres...")
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            # Création de la table avec les types de données appropriés
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id VARCHAR(50) PRIMARY KEY,
                    customer_name VARCHAR(100),
                    product_name VARCHAR(100),
                    price FLOAT,
                    order_date TIMESTAMP
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("✅ Postgres est prêt !")
            break
        except Exception as e:
            print(f"⏳ Attente de Postgres... ({e})")
            time.sleep(2)

# Exécution de la configuration de la base de données au démarrage
setup_database()

print("🚀 Consumer démarré...")
try:
    # Initialisation du Consumer Kafka pour écouter le topic 'ecommerce-orders'
    consumer = KafkaConsumer(
        'ecommerce-orders',
        bootstrap_servers=['localhost:9092'],
        # 'earliest' permet de lire tous les messages depuis le début du topic
        auto_offset_reset='earliest', 
        # On change le group_id pour forcer Kafka à relire les anciennes données avec le nouveau mapping
        group_id='ecommerce-recording-group-v3', 
        # Désérialisation du message JSON reçu de Kafka
        value_deserializer=lambda x: json.loads(x.decode('utf-8', 'ignore'))
    )

    # Boucle infinie pour traiter chaque message reçu
    for message in consumer:
        data = message.value
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Insertion des données dans PostgreSQL
            # IMPORTANT : Les clés ('customer', 'product', 'timestamp') doivent correspondre 
            # exactement à celles envoyées par le Producer.
            cur.execute(
                "INSERT INTO orders (order_id, customer_name, product_name, price, order_date) VALUES (%s, %s, %s, %s, %s)",
                (
                    data.get('order_id'),    # ID unique de la commande
                    data.get('customer'),    # Nom du client (Clé corrigée pour matcher le Producer)
                    data.get('product'),     # Nom du produit (Clé corrigée)
                    data.get('price'),       # Prix du produit
                    data.get('timestamp')    # Date et heure (Clé corrigée)
                )
            )
            
            conn.commit()
            cur.close()
            conn.close()
            print(f"✅ Commande enregistrée en base : {data.get('order_id')}")
        except Exception as e:
            print(f"❌ Erreur lors de l'insertion SQL : {e}")
            
except Exception as e:
    print(f"❌ Erreur critique du Consumer Kafka : {e}")

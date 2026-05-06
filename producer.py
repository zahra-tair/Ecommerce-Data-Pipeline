import requests
import time
import json
from kafka import KafkaProducer

def get_producer():
    """Initialise la connexion avec le broker Kafka avec gestion de l'attente."""
    print("⏳ Connexion à Kafka en cours (127.0.0.1:9092)...")
    while True:
        try:
            # On utilise l'IP 127.0.0.1 pour éviter les lenteurs DNS de localhost
            p = KafkaProducer(
                bootstrap_servers=['127.0.0.1:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                api_version=(0, 10, 1),
                # Augmentation des délais pour laisser le temps à Kafka de répondre
                request_timeout_ms=120000,
                max_block_ms=120000
            )
            print("✅ Connecté à Kafka avec succès !")
            return p
        except Exception:
            print(" Kafka n'est pas encore prêt, nouvelle tentative dans 5s...")
            time.sleep(5)

def fetch_and_send():
    """Boucle principale pour récupérer les données des APIs et les envoyer à Kafka."""
    producer = get_producer()
    print(" Démarrage de l'envoi des données (Ctrl+C pour arrêter)...")
    
    while True:
        try:
            # 1. Récupération des données produit depuis Fake Store API
            # On génère un ID de produit entre 1 et 20 dynamiquement
            product_id = int(time.time() % 20) + 1 
            product_res = requests.get(f'https://fakestoreapi.com/products/{product_id}').json()
            
            # 2. Récupération d'un utilisateur aléatoire depuis Random User API
            user_res = requests.get('https://randomuser.me/api/').json()['results'][0]
            
            # 3. Construction de l'objet de transaction (Commande)
            # On regroupe les informations des deux APIs dans un seul dictionnaire
            order = {
                'order_id': int(time.time()),
                'customer': f"{user_res['name']['first']} {user_res['name']['last']}",
                'product': product_res['title'],
                'price': product_res['price'],
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 4. Envoi du message JSON vers le topic Kafka 'ecommerce-orders'
            producer.send('ecommerce-orders', value=order)
            print(f"📦 Commande envoyée : {order['customer']} a acheté {order['product']}")
            
            # Pause de 3 secondes pour simuler un flux continu et raisonnable
            time.sleep(3)
            
        except Exception as e:
            # En cas d'erreur (connexion API ou autre), on affiche l'erreur et on attend 5s
            print(f"⚠️ Erreur lors du traitement : {e}")
            time.sleep(5)

if __name__ == "__main__":
    fetch_and_send()
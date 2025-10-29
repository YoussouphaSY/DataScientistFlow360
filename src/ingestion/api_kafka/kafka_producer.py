import time
import pandas as pd
from kafka import KafkaProducer
import json
import os
from datetime import datetime

# === CONFIGURATION ===
CSV_PATH = "/collection/senegal_hospital_data_2024.csv"
ARCHIVE_DIR = "/collection/archive"
BATCH_SIZE = 50
TOPIC = "data_stream"
KAFKA_BOOTSTRAP = ['kafka:9092']

# Créer le dossier d'archive s'il n'existe pas
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ================================
# Connexion à Kafka avec retry
# ================================
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connecté à Kafka !")
    except Exception as e:
        print(f"Kafka pas encore prêt, retry dans 5 secondes... ({e})")
        time.sleep(5)

# ================================
# Fonction principale
# ================================
def stream_csv_to_kafka():
    if not os.path.exists(CSV_PATH):
        print("Fichier source introuvable.")
        return

    df = pd.read_csv(CSV_PATH)

    if df.empty:
        print("Aucun enregistrement à envoyer.")
        return

    print(f"{len(df)} lignes trouvées dans le fichier à streamer...")

    total_sent = 0
    while not df.empty:
        batch = df.iloc[:BATCH_SIZE]
        remaining = df.iloc[BATCH_SIZE:]

        # Envoi du batch dans Kafka
        producer.send(TOPIC, value=batch.to_dict(orient="records"))
        producer.flush()
        print(f"Envoyé {len(batch)} lignes vers Kafka ({total_sent + len(batch)}/{len(df) + total_sent})")

        # Archiver les lignes envoyées
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = os.path.join(ARCHIVE_DIR, f"archived_batch_{timestamp}.csv")
        batch.to_csv(archive_path, index=False)
        print(f"Batch archivé → {archive_path}")

        # Mise à jour du CSV original
        if not remaining.empty:
            remaining.to_csv(CSV_PATH, index=False)
        else:
            os.remove(CSV_PATH)
            print("Fichier source vidé et supprimé.")
            break

        total_sent += len(batch)
        df = remaining

        time.sleep(5)  # Pause avant le prochain batch

    print("Streaming terminé et toutes les données ont été archivées.")

# ================================
# Démarrage du streaming
# ================================
if __name__ == "__main__":
    stream_csv_to_kafka()

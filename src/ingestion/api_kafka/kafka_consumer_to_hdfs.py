from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import pandas as pd
import time
import yaml
import logging
import os

# ---------------------- Configuration Logging ----------------------
log_file = "hdfs_ingestion.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logging.info("Démarrage de l'ingestion Kafka → HDFS...")

# ---------------------- Charger config HDFS ----------------------
cfg = yaml.safe_load(open("config.yaml"))
hdfs_client = InsecureClient(cfg["hdfs"]["url"], user="root")

# ---------------------- Configuration Kafka ----------------------
KAFKA_BROKER = "kafka:9092"
TOPIC = "data_stream"
GROUP_ID = "hdfs_loader"

# ---------------------- Retry pour Kafka ----------------------
while True:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=GROUP_ID
        )
        logging.info("Connecté à Kafka !")
        break
    except Exception as e:
        logging.warning(f"Kafka pas encore prêt, retry dans 5 secondes... ({e})")
        time.sleep(5)

# ---------------------- Consommation et écriture HDFS ----------------------
for message in consumer:
    try:
        # Ignorer les messages vides
        if not message.value:
            logging.warning("Message vide reçu — ignoré.")
            continue

        records = message.value

        # S'assurer que les données sont une liste de dictionnaires
        if isinstance(records, dict):
            records = [records]
        elif not isinstance(records, list):
            logging.warning(f"Format inattendu du message : {type(records)} — ignoré.")
            continue

        # Vérifier que la liste n’est pas vide
        if not records:
            logging.warning("Liste vide reçue — ignorée.")
            continue

        # Convertir en DataFrame
        try:
            df = pd.DataFrame(records)
        except Exception as e:
            logging.error(f"Erreur DataFrame : {e}")
            continue

        if df.empty:
            logging.warning("Batch vide, rien à écrire dans HDFS.")
            continue

        # Générer un nom de fichier unique
        file_path = f"/data_streaming/raw/stream_batch_{int(time.time())}_{os.urandom(4).hex()}.csv"

        # Écrire dans HDFS
        while True:
            try:
                with hdfs_client.write(file_path, encoding="utf-8") as writer:
                    df.to_csv(writer, index=False)
                logging.info(f"Batch écrit dans HDFS : {file_path}")
                break
            except Exception as e:
                if "Name node is in safe mode" in str(e):
                    logging.warning("HDFS en Safe Mode, attente de 5 secondes...")
                    time.sleep(5)
                elif "already exists" in str(e):
                    logging.warning(f"Fichier {file_path} existe déjà, création d’un nouveau nom...")
                    file_path = f"/data_streaming/raw/stream_batch_{int(time.time())}_{os.urandom(4).hex()}.csv"
                else:
                    logging.error(f"Erreur inattendue lors de l'écriture dans HDFS: {e}")
                    raise e

    except Exception as e:
        logging.error(f"Erreur dans le traitement du batch Kafka: {e}")

logging.info("Ingestion Kafka → HDFS terminée.")

import os
import time
import logging
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from hdfs import InsecureClient

# ---------------- Désactiver métriques JVM/JMX ----------------
os.environ["SPARK_JAVA_OPTS"] = "-Dspark.executor.metrics.enabled=false -Dspark.driver.metrics.enabled=false"

# ---------------- Configuration logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------- Charger la configuration ----------------
cfg = yaml.safe_load(open("config.yaml"))

# Utiliser le vrai port HDFS RPC (8020)
HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")

RAW_PATH = f"{HDFS_URL}{cfg['hdfs']['path']}"
CLEAN_PATH = f"{HDFS_URL}{cfg['hdfs']['cleaned_path']}"

# Dossier local du projet pour sauvegarder les fichiers nettoyés
LOCAL_CLEAN_DIR = "./data/clean"

# ---------------- Initialiser Spark ----------------
spark = (
    SparkSession.builder
    .appName("DataCleaningPipeline")
    .config("spark.hadoop.fs.defaultFS", HDFS_URL)
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.ui.explainMode", "simple")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------- Client HDFS ----------------
hdfs_client = InsecureClient(cfg["hdfs"]["url"], user="root")


# ---------------- Réinitialiser les dossiers clean ----------------
def reset_clean_folders():
    """Supprime et recrée les dossiers clean (HDFS + local)."""
    # --- Supprimer le dossier HDFS clean ---
    try:
        if hdfs_client.status(cfg["hdfs"]["cleaned_path"], strict=False):
            logging.info(f"Suppression de l'ancien dossier HDFS : {cfg['hdfs']['cleaned_path']}")
            hdfs_client.delete(cfg["hdfs"]["cleaned_path"], recursive=True)
        # Recréer le dossier
        hdfs_client.makedirs(cfg["hdfs"]["cleaned_path"])
        logging.info(f"Dossier HDFS recréé : {cfg['hdfs']['cleaned_path']}")
    except Exception as e:
        logging.error(f"Erreur lors de la réinitialisation du dossier HDFS : {e}")

    # --- Supprimer le dossier local clean ---
    try:
        if os.path.exists(LOCAL_CLEAN_DIR):
            logging.info(f"Suppression du dossier local existant : {LOCAL_CLEAN_DIR}")
            for root, dirs, files in os.walk(LOCAL_CLEAN_DIR, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(LOCAL_CLEAN_DIR)
        os.makedirs(LOCAL_CLEAN_DIR, exist_ok=True)
        logging.info(f"Dossier local recréé : {LOCAL_CLEAN_DIR}")
    except Exception as e:
        logging.error(f"Erreur lors de la réinitialisation du dossier local : {e}")


# ---------------- Fonction de nettoyage ----------------
def clean_dataframe(df):
    """Nettoyage générique : suppression doublons, trim, lower..."""
    df = df.dropDuplicates()
    df = df.dropna(how='all')

    # Nettoyer les noms de colonnes
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().replace(" ", "_").lower())

    # Nettoyer le contenu
    for c in df.columns:
        df = df.withColumn(c, trim(lower(col(c))))

    return df


# ---------------- Traitement batch des fichiers CSV ----------------
def process_files_individually():
    logging.info("Début du nettoyage individuel des fichiers CSV...")

    files = [f for f in hdfs_client.list(cfg["hdfs"]["path"]) if f.endswith(".csv")]
    if not files:
        logging.warning("Aucun fichier CSV trouvé dans le dossier brut.")
        return

    # Réinitialiser les dossiers avant nettoyage
    reset_clean_folders()

    for file_name in files:
        raw_file_path = f"{RAW_PATH}/{file_name}"
        clean_file_path = f"{CLEAN_PATH}/{file_name.replace('.csv', '_clean.csv')}"

        try:
            logging.info(f"Lecture du fichier {file_name} depuis HDFS...")
            df = spark.read.option("header", "true").csv(raw_file_path)
            logging.info(f"{file_name} : {df.count()} lignes, {len(df.columns)} colonnes détectées.")

            # Nettoyage
            df_clean = clean_dataframe(df)

            # Sauvegarde HDFS
            df_clean.write.mode("overwrite").option("header", "true").csv(clean_file_path)
            logging.info(f"Fichier nettoyé sauvegardé dans HDFS : {clean_file_path}")

            # Sauvegarde locale
            local_file_path = os.path.join(LOCAL_CLEAN_DIR, f"clean_{file_name}")
            df_clean.toPandas().to_csv(local_file_path, index=False)
            logging.info(f"Fichier nettoyé enregistré localement : {local_file_path}")

        except Exception as e:
            logging.error(f"Erreur lors du traitement de {file_name} : {e}")


# ---------------- Traitement streaming ----------------
def process_streaming():
    logging.info("Début du traitement en temps réel...")

    STREAMING_PATH = f"{HDFS_URL}/data_streaming"
    CLEAN_STREAM_PATH = f"{HDFS_URL}/data_streaming_clean"

    try:
        # Lire le flux CSV en temps réel
        df_stream = (
            spark.readStream
            .option("header", "true")
            .csv(STREAMING_PATH)
        )

        # Appliquer la fonction de nettoyage
        df_clean_stream = clean_dataframe(df_stream)

        # Écriture du flux nettoyé en mode append avec checkpoint
        query = (
            df_clean_stream.writeStream
            .outputMode("append")
            .option("checkpointLocation", f"{CLEAN_STREAM_PATH}/_checkpoints")
            .format("csv")
            .option("path", CLEAN_STREAM_PATH)
            .start()
        )

        logging.info(f"Streaming actif vers : {CLEAN_STREAM_PATH}")
        query.awaitTermination()

    except Exception as e:
        logging.error(f"Erreur lors du streaming : {e}")


# ---------------- Boucle principale ----------------
if __name__ == "__main__":
    # Nettoyage batch
    process_files_individually()

    # Nettoyage streaming (temps réel)
    process_streaming()

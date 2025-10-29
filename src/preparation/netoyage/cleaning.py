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

RAW_PATH = f"hdfs://namenode:9870{cfg['hdfs']['path']}"
CLEAN_PATH = f"hdfs://namenode:9870/dataflow360/clean/"

# ---------------- Initialiser Spark ----------------
spark = (
    SparkSession.builder
    .appName("DataCleaningPipeline")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9870")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.ui.explainMode", "simple")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------- Client HDFS ----------------
hdfs_client = InsecureClient(cfg["hdfs"]["url"], user="root")

# Créer le dossier clean s'il n'existe pas
try:
    if not hdfs_client.status("/dataflow360/clean", strict=False):
        logging.info("Création du dossier HDFS /dataflow360/clean")
        hdfs_client.makedirs("/dataflow360/clean")
except Exception as e:
    logging.error(f"Erreur lors de la création du dossier HDFS : {e}")

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

# ---------------- Traitement des fichiers ----------------
def process_files():
    logging.info("Lecture des fichiers bruts depuis HDFS...")
    df = spark.read.option("header", "true").csv(RAW_PATH)
    logging.info(f"{df.count()} lignes chargées.")

    df_clean = clean_dataframe(df)
    logging.info("Nettoyage terminé.")

    # Écriture dans le dossier clean
    df_clean.write.mode("overwrite").option("header", "true").csv(CLEAN_PATH)
    logging.info(f"Données nettoyées enregistrées dans {CLEAN_PATH}")

# ---------------- Boucle principale ----------------
if __name__ == "__main__":
    while True:
        process_files()
        logging.info("Prochaine vérification dans 5 minutes...")
        time.sleep(300)

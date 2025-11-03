import os
import logging
import yaml
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import (
    StructType, StringType, IntegerType, FloatType, DateType, BooleanType
)
import psycopg2

# ---------------- Chargement config ----------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

HDFS_URL = config["hdfs_url"]
HDFS_STREAM_RAW = f"{HDFS_URL}{config['hdfs_stream_raw_path']}"
HDFS_STREAM_CLEAN = f"{HDFS_URL}{config['hdfs_stream_clean_path']}"
CHECKPOINT_DIR = config["checkpoint_dir"]
LOCAL_OUTPUT_PATH = config["local_clean_path"]
LOG_FILE = config["log_file"]

POSTGRES = config["postgres"]

os.makedirs(LOCAL_OUTPUT_PATH, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# ---------------- Logging ----------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------- Spark ----------------
spark = SparkSession.builder \
    .appName("DataFlow360_Streaming_Cleaner") \
    .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

client = InsecureClient("http://namenode:9870", user="root")

# ---------------- Schéma explicite ----------------
schema = StructType() \
    .add("hospital_id", IntegerType()) \
    .add("hospital_name", StringType()) \
    .add("region", StringType()) \
    .add("city", StringType()) \
    .add("capacity_beds", IntegerType()) \
    .add("capacity_staff", IntegerType()) \
    .add("date", DateType()) \
    .add("patients_admitted", IntegerType()) \
    .add("patients_released", IntegerType()) \
    .add("patients_in_care", IntegerType()) \
    .add("icu_patients", IntegerType()) \
    .add("available_beds", IntegerType()) \
    .add("available_staff", IntegerType()) \
    .add("emergency_cases", IntegerType()) \
    .add("mortality_rate", FloatType()) \
    .add("disease_name", StringType()) \
    .add("new_cases", IntegerType()) \
    .add("new_deaths", IntegerType()) \
    .add("vaccination_rate", FloatType()) \
    .add("test_positivity_rate", FloatType()) \
    .add("risk_level", StringType()) \
    .add("temperature_c", FloatType()) \
    .add("humidity_percent", FloatType()) \
    .add("population_density", FloatType()) \
    .add("public_alert", BooleanType()) \
    .add("region_mobility_index", FloatType()) \
    .add("medical_supplies_index", FloatType()) \
    .add("staff_absence_rate", FloatType()) \
    .add("avg_wait_time_minutes", FloatType()) \
    .add("response_time_minutes", FloatType())

# ---------------- Nettoyage ----------------
def clean_dataframe(df):
    """Remplace les chaînes vides par None et supprime les lignes entièrement nulles"""
    for c in df.columns:
        df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))
    return df.dropna(how="all")

# ---------------- PostgreSQL ----------------
def create_db_and_table():
    """Crée la base et la table si elles n'existent pas"""
    try:
        # Connexion à une DB existante (postgres) pour créer la nouvelle DB
        conn = psycopg2.connect(
            host=POSTGRES['host'],
            port=POSTGRES['port'],
            user=POSTGRES['user'],
            password=POSTGRES['password'],
            database="postgres"
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Créer la base si elle n'existe pas
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{POSTGRES['db']}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {POSTGRES['db']}")
            logging.info(f"✅ Base de données {POSTGRES['db']} créée")
        cur.close()
        conn.close()

        # Connexion à la base nouvellement créée
        conn = psycopg2.connect(
            host=POSTGRES['host'],
            port=POSTGRES['port'],
            user=POSTGRES['user'],
            password=POSTGRES['password'],
            database=POSTGRES['db']
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Créer la table si elle n'existe pas
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES['table']} (
            hospital_id INT,
            hospital_name TEXT,
            region TEXT,
            city TEXT,
            capacity_beds INT,
            capacity_staff INT,
            date DATE,
            patients_admitted INT,
            patients_released INT,
            patients_in_care INT,
            icu_patients INT,
            available_beds INT,
            available_staff INT,
            emergency_cases INT,
            mortality_rate FLOAT,
            disease_name TEXT,
            new_cases INT,
            new_deaths INT,
            vaccination_rate FLOAT,
            test_positivity_rate FLOAT,
            risk_level TEXT,
            temperature_c FLOAT,
            humidity_percent FLOAT,
            population_density FLOAT,
            public_alert BOOLEAN,
            region_mobility_index FLOAT,
            medical_supplies_index FLOAT,
            staff_absence_rate FLOAT,
            avg_wait_time_minutes FLOAT,
            response_time_minutes FLOAT
        );
        """
        cur.execute(create_table_sql)
        logging.info(f"✅ Table {POSTGRES['table']} créée si inexistante")
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"❌ Erreur création DB/table: {e}")

def save_to_postgres(df, mode="append"):
    """Sauvegarde un DataFrame Spark dans PostgreSQL"""
    url = f"jdbc:postgresql://{POSTGRES['host']}:{POSTGRES['port']}/{POSTGRES['db']}"
    properties = {"user": POSTGRES["user"], "password": POSTGRES["password"], "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url, table=POSTGRES["table"], mode=mode, properties=properties)
    logging.info(f"✅ Données sauvegardées dans PostgreSQL table: {POSTGRES['table']}")

# ---------------- Streaming ----------------
def process_streaming():
    logging.info("🚀 Démarrage du streaming...")

    try:
        # Lecture streaming depuis HDFS avec schéma explicite
        df_stream = spark.readStream \
            .option("header", "true") \
            .schema(schema) \
            .csv(HDFS_STREAM_RAW)

        df_clean_stream = clean_dataframe(df_stream)

        # Sauvegarde HDFS (format Parquet)
        query_hdfs = df_clean_stream.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", HDFS_STREAM_CLEAN) \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .start()

        # Sauvegarde PostgreSQL
        def foreach_batch_function(batch_df, batch_id):
            batch_df_clean = clean_dataframe(batch_df)
            save_to_postgres(batch_df_clean)

        query_pg = df_clean_stream.writeStream \
            .foreachBatch(foreach_batch_function) \
            .option("checkpointLocation", CHECKPOINT_DIR + "_pg") \
            .start()

        query_hdfs.awaitTermination()
        query_pg.awaitTermination()

    except Exception as e:
        logging.error(f"❌ Erreur streaming : {e}")

# ---------------- Main ----------------
if __name__ == "__main__":
    logging.info("=== Nettoyage streaming lancé ===")
    create_db_and_table()
    process_streaming()

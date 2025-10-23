import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
from hdfs import InsecureClient
import yaml
import logging
import time

# =====================================================
# CONFIGURATION LOGGING
# =====================================================
logging.basicConfig(
    filename="load_to_hdfs.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =====================================================
# CHARGEMENT CONFIGURATION
# =====================================================
cfg = yaml.safe_load(open("config.yaml"))

hdfs_client = InsecureClient(cfg["hdfs"]["url"], user="root")

# =====================================================
# ATTENTE SI NAMENODE EN SAFE MODE
# =====================================================
try:
    # On essaie plusieurs fois jusqu’à ce que HDFS soit prêt
    for _ in range(10):
        try:
            status = hdfs_client.status('/', strict=False)
            # Si pas d’erreur, HDFS est accessible
            break
        except Exception as e:
            logging.error(f"Erreur lors de la vérification du safe mode : {e}")
            time.sleep(5)
except Exception as e:
    logging.error(f"Impossible de contacter HDFS : {e}")


# =====================================================
# --- FONCTION UTILITAIRE POUR ÉCRITURE DANS HDFS ---
# =====================================================
def write_to_hdfs(file_path, df):
    """Supprime le fichier s'il existe, puis écrit dans HDFS."""
    try:
        # Supprimer le fichier s’il existe déjà
        if hdfs_client.status(file_path, strict=False):
            hdfs_client.delete(file_path)
        # Écriture du DataFrame
        with hdfs_client.write(file_path, encoding="utf-8") as writer:
            df.to_csv(writer, index=False)
    except Exception as e:
        raise RuntimeError(f"Erreur lors de l'écriture dans HDFS : {e}")


# =====================================================
# --- MYSQL ---
# =====================================================
try:
    mysql_engine = create_engine(
        f"mysql+pymysql://{cfg['mysql']['user']}:{cfg['mysql']['password']}@"
        f"{cfg['mysql']['host']}:{cfg['mysql']['port']}/{cfg['mysql']['database']}"
    )

    tables = pd.read_sql("SHOW TABLES", mysql_engine)["Tables_in_sante_db"]

    time.sleep(20)  # petite pause avant l’écriture
    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", mysql_engine)
            file_path = f"{cfg['hdfs']['path']}mysql_{table}.csv"
            write_to_hdfs(file_path, df)
        except Exception as e:
            logging.error(f"Failed to process MySQL table {table}: {e}")
except Exception as e:
    logging.error(f"Failed to connect to MySQL: {e}")


# =====================================================
# --- POSTGRESQL ---
# =====================================================
try:
    pg_engine = create_engine(
        f"postgresql+psycopg2://{cfg['postgres']['user']}:{cfg['postgres']['password']}@"
        f"{cfg['postgres']['host']}:{cfg['postgres']['port']}/{cfg['postgres']['database']}"
    )

    tables = pd.read_sql(
        "SELECT tablename FROM pg_tables WHERE schemaname='public';",
        pg_engine
    )["tablename"]

    time.sleep(10)
    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", pg_engine)
            file_path = f"{cfg['hdfs']['path']}postgres_{table}.csv"
            write_to_hdfs(file_path, df)
        except Exception as e:
            logging.error(f"Failed to process PostgreSQL table {table}: {e}")
except Exception as e:
    logging.error(f"Failed to connect to PostgreSQL: {e}")


# =====================================================
# --- MONGODB ---
# =====================================================
try:
    mongo_client = MongoClient(
        f"mongodb://{cfg['mongo']['user']}:{cfg['mongo']['password']}@"
        f"{cfg['mongo']['host']}:{cfg['mongo']['port']}/"
    )
    db = mongo_client[cfg["mongo"]["database"]]
    collections = db.list_collection_names()

    time.sleep(10)
    for collection in collections:
        try:
            docs = list(db[collection].find())
            df = pd.DataFrame(docs)
            file_path = f"{cfg['hdfs']['path']}mongo_{collection}.csv"
            write_to_hdfs(file_path, df)
        except Exception as e:
            logging.error(f"Failed to process MongoDB collection {collection}: {e}")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")

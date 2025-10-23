# =====================================================
# AUTOMATISATION TOTALE : MySQL, PostgreSQL, MongoDB
# =====================================================

import os
import pandas as pd
import mysql.connector
import psycopg2
from pymongo import MongoClient
import time

# =====================================================
# ⚙️ CONFIGURATIONS DES BASES DE DONNÉES
# =====================================================

MYSQL_CONFIG = {
    "host": "dataflow360_mysql",     
    "user": "root",              
    "password": "mysql_admin123", 
    "database": "sante_db",       
    "port": 3306
}

POSTGRES_CONFIG = {
    "host": "dataflow360_postgres",   
    "user": "postgres_admin",  
    "password": "postgres_admin123",
    "dbname": "sante_db",          # Nom de la base
    "port": 5432
}

MONGO_CONFIG = {
    "host": "dataflow360_mongo",      
    "port": 27017,                      
    "username": "mongo_admin",           
    "password": "mongo_admin123" , 
    "database": "sante_db"        
}


# =====================================================
# MYSQL : création de la base si elle n’existe pas
# =====================================================
def ensure_mysql_database():
    """Créer la base MySQL si elle n'existe pas"""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG["host"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"],
            port=MYSQL_CONFIG["port"]
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_CONFIG['database']}")
        print(f"Base MySQL '{MYSQL_CONFIG['database']}' vérifiée/créée.")
    except mysql.connector.Error as err:
        print(f"Erreur création MySQL : {err}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# =====================================================
# POSTGRESQL : création de la base si elle n’existe pas
# =====================================================
def ensure_postgres_database():
    """Créer la base PostgreSQL si elle n'existe pas"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG["host"],
            user=POSTGRES_CONFIG["user"],
            password=POSTGRES_CONFIG["password"],
            port=POSTGRES_CONFIG["port"],
            dbname="postgres"  # Connexion à la base par défaut
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{POSTGRES_CONFIG['dbname']}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f"CREATE DATABASE {POSTGRES_CONFIG['dbname']}")
            print(f"Base PostgreSQL '{POSTGRES_CONFIG['dbname']}' créée.")
        else:
            print(f"Base PostgreSQL '{POSTGRES_CONFIG['dbname']}' existe déjà.")
    except Exception as e:
        print(f"❌ Erreur création PostgreSQL : {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# =====================================================
# MYSQL : création automatique de la table
# =====================================================
def create_mysql_table(dataframe, table_name):
    """Créer la table MySQL si elle n'existe pas en fonction du DataFrame"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        columns = []
        for col, dtype in zip(dataframe.columns, dataframe.dtypes):
            if "int" in str(dtype): sql_type = "INT"
            elif "float" in str(dtype): sql_type = "FLOAT"
            else: sql_type = "VARCHAR(255)"
            columns.append(f"`{col}` {sql_type}")

        columns_sql = ", ".join(columns)
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, {columns_sql})"
        cursor.execute(sql)
        conn.commit()
        print(f"Table MySQL '{table_name}' vérifiée/créée.")
    except Exception as e:
        print(f"Erreur création table MySQL : {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# =====================================================
# POSTGRESQL : création automatique de la table
# =====================================================
def create_postgres_table(dataframe, table_name):
    """Créer la table PostgreSQL si elle n'existe pas en fonction du DataFrame"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        columns = []
        for col, dtype in zip(dataframe.columns, dataframe.dtypes):
            if "int" in str(dtype): sql_type = "INT"
            elif "float" in str(dtype): sql_type = "FLOAT"
            else: sql_type = "VARCHAR(255)"
            columns.append(f"{col} {sql_type}")

        columns_sql = ", ".join(columns)
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {columns_sql})"
        cursor.execute(sql)
        conn.commit()
        print(f"Table PostgreSQL '{table_name}' vérifiée/créée.")
    except Exception as e:
        print(f"Erreur création table PostgreSQL : {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# =====================================================
# INSERTION MYSQL
# =====================================================
def insert_into_mysql(dataframe, table_name):
    conn = None
    cursor = None
    try:
        ensure_mysql_database()
        create_mysql_table(dataframe, table_name)
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Vérifie si la table contient déjà des données
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Table '{table_name}' déjà remplie, insertion ignorée.")
            return

        # Insertion des données
        for _, row in dataframe.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            columns = ", ".join([f"`{col}`" for col in row.index])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))
        conn.commit()
        print(f"Données insérées dans MySQL ({table_name})")

    except Exception as e:
        print(f"MySQL Error : {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


# =====================================================
# INSERTION POSTGRESQL
# =====================================================
def insert_into_postgres(dataframe, table_name):
    conn = None
    cursor = None
    try:
        ensure_postgres_database()
        create_postgres_table(dataframe, table_name)
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        # Vérifie si la table contient déjà des données
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Table '{table_name}' déjà remplie, insertion ignorée.")
            return

        # Insertion des données
        for _, row in dataframe.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            columns = ", ".join(row.index)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))
        conn.commit()
        print(f"Données insérées dans PostgreSQL ({table_name})")

    except Exception as e:
        print(f"PostgreSQL Error : {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


# =====================================================
# INSERTION MONGODB
# =====================================================
def insert_into_mongo(collection_name, dataframe):
    client = None
    try:
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        client = MongoClient(uri)
        db = client[MONGO_CONFIG["database"]]
        collection = db[collection_name]

        # Vérifie si la collection contient déjà des documents
        if collection.count_documents({}) > 0:
            print(f"Collection '{collection_name}' déjà remplie, insertion ignorée.")
            return

        # Insertion des données
        collection.insert_many(dataframe.to_dict(orient="records"))
        print(f"Données insérées dans MongoDB ({collection_name})")

    except Exception as e:
        print(f"MongoDB Error : {e}")
    finally:
        if client: client.close()


# =====================================================
# FONCTION PRINCIPALE
# =====================================================

def main():

    print("Attente du démarrage des bases de données...")
    time.sleep(15)  # attend 15 secondes
    print("Début de l’exécution du collecteur...")

    raw_data_dir = "data"

    # Dictionnaire qui mappe chaque fichier CSV à une base
    file_to_db = {
        "healthcare_dataset.csv": "mysql",
        "public_health_surveillance_dataset.csv": "postgres",
        "phof-datase.csv": "mongo"
    }

    for file in os.listdir(raw_data_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(raw_data_dir, file)
            print(f"Traitement du fichier : {file_path}")

            df = pd.read_csv(file_path)
            table_name = os.path.splitext(file)[0].replace("-", "_")

            # Vérifie la base cible et insère dans la bonne
            target = file_to_db.get(file)
            if target == "mysql":
                insert_into_mysql(df, table_name)
            elif target == "postgres":
                insert_into_postgres(df, table_name)
            elif target == "mongo":
                insert_into_mongo(table_name, df)
            else:
                print(f"Aucune base définie pour le fichier {file}")


# =====================================================
# POINT D’ENTRÉE
# =====================================================
if __name__ == "__main__":
    main()

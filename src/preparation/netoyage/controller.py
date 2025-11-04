import os
import time
import yaml
import logging
import subprocess
import papermill as pm

# ---------------- Chargement config ----------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

LOCAL_CLEAN_PATH = "/app/data/clean"       # dossier monté depuis Docker
NOTEBOOK_TEMPLATE = "/app/notebooks/eda_notebook.ipynb"
EDA_OUTPUT_FOLDER = "/app/eda_reports"
LOG_FILE = "/app/logs/controller.log"

os.makedirs(EDA_OUTPUT_FOLDER, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# ---------------- Logging ----------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------- Fonction EDA ----------------
def run_eda_for_file(csv_path):
    """Exécute le notebook EDA pour un CSV et génère le rapport HTML"""
    base_name = os.path.basename(csv_path).replace(".csv", "")
    output_nb = os.path.join(EDA_OUTPUT_FOLDER, f"{base_name}_EDA.ipynb")
    output_html = os.path.join(EDA_OUTPUT_FOLDER, f"{base_name}_report.html")

    if not os.path.exists(NOTEBOOK_TEMPLATE):
        logging.error(f"❌ Notebook EDA introuvable : {NOTEBOOK_TEMPLATE}")
        return

    try:
        logging.info(f"📊 Lancement EDA pour {csv_path}")
        pm.execute_notebook(
            NOTEBOOK_TEMPLATE,
            output_nb,
            parameters={"input_path": csv_path},
            log_output=True
        )
        subprocess.run(["jupyter", "nbconvert", "--to", "html", output_nb, "--output", output_html], check=True)
        logging.info(f"✅ Rapport généré : {output_html}")
    except Exception as e:
        logging.error(f"❌ Erreur EDA: {e}")

# ---------------- Boucle principale améliorée ----------------
processed_files = set()
WAIT_TIME = 5        # temps d'attente entre vérifications
MAX_WAIT = 300       # timeout en secondes si le fichier n'apparaît pas

if __name__ == "__main__":
    logging.info("=== Controller EDA DataFlow360 démarré ===")
    while True:
        try:
            files = [f for f in os.listdir(LOCAL_CLEAN_PATH) if f.endswith("_clean.csv")]
            new_files = [f for f in files if f not in processed_files]

            for f in new_files:
                csv_path = os.path.join(LOCAL_CLEAN_PATH, f)

                # ---- Attente que le fichier soit prêt ----
                elapsed = 0
                while not os.path.exists(csv_path):
                    if elapsed > MAX_WAIT:
                        logging.error(f"⏱ Timeout : {csv_path} introuvable après {MAX_WAIT}s")
                        break
                    logging.info(f"⏳ {csv_path} non encore disponible, attente {WAIT_TIME}s...")
                    time.sleep(WAIT_TIME)
                    elapsed += WAIT_TIME
                else:
                    # ---- Lancement EDA ----
                    run_eda_for_file(csv_path)
                    processed_files.add(f)

            time.sleep(30)
        except Exception as e:
            logging.error(f"⚠️ Erreur boucle principale : {e}")
            time.sleep(10)

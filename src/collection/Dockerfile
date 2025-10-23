FROM python:3.12-slim

# Définir le répertoire de travail
WORKDIR /app 

# Copier les fichiers nécessaires
COPY requirements.txt .
COPY . .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Commande par défaut pour exécuter le script
CMD ["python", "collect_data.py"]
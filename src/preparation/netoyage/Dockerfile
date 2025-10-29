# --------------------- Image de base : Java 11 + Spark + Python ---------------------
FROM openjdk:11-jdk-slim

# Définir variables d'environnement Spark/Hadoop
ENV SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin

# Installer dépendances système + Python + ps (pour Spark)
RUN apt-get update && apt-get install -y curl python3 python3-pip procps && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip install --upgrade pip

# Télécharger Spark
RUN curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark.tgz

# Définir le répertoire de travail
WORKDIR /app

# Copier et installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste des fichiers
COPY . .

# Lancer le script de nettoyage
CMD ["python", "cleaning.py"]

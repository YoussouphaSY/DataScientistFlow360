# ---------------- Base image ----------------
FROM openjdk:11-jdk-slim

# ---------------- Installer Python et dépendances système ----------------
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    procps \
    build-essential \
    libpq-dev \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && pip install --upgrade pip

# ---------------- Installer packages Python ----------------
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pandas==2.2.2 \
    pyarrow==15.0.0 \
    pyyaml \
    psycopg2-binary \
    hdfs \
    papermill \
    jupyter \
    matplotlib \
    seaborn \
    SQLAlchemy

# ---------------- Installer Spark ----------------
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$JAVA_HOME/bin:$PATH:$SPARK_HOME/bin

RUN curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark.tgz

# ---------------- Ajouter JAR PostgreSQL pour PySpark ----------------
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/postgresql-42.6.0.jar pyspark-shell"

# ---------------- Répertoire de travail ----------------
WORKDIR /app
COPY . .

# ---------------- Commande par défaut ----------------
CMD ["python", "cleaning.py"]

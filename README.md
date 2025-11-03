docker compose build -no--cache
docker compose up -d
docker logs -f dataflow360_spark_cleaner


- mmongosh -u mongo_admin -p mongo_admin123
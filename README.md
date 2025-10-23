## psql -U postgres_admin -d 

## mongosh -u mongo_admin -p mongo_admin123 --authenticationDatabase admin


## Mes datasets :
- https://www.kaggle.com/datasets/datasetengineer/public-health-dataset
- https://www.kaggle.com/datasets/prasad22/healthcare-dataset
- https://nihr.opendatasoft.com/explore/dataset/phof-datase/information/?disjunctive.programme&disjunctive.funding_stream&disjunctive.status



docker-compose down
docker-compose build --no-cache
docker-compose up -d
docker logs -f dataflow360_collector

docker compose down 
docker compose build --no-cache
docker compose up -d
docker logs -f dataflow360_api_kafka
 
docker compose down
docker compose build --no-cache streaming_app
docker compose up -d streaming_app
docker logs -f dataflow360_api_kafka

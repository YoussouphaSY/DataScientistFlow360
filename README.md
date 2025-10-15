DataScientistFlow360/
│
├── config/                         
│   ├── settings.yaml               # Config globale (ports, connexions DB)
│   ├── kafka_config.yaml           
│   ├── airflow_config.yaml
│   ├── mlflow_config.yaml
│   ├── prefect_config.yaml
│   ├── prometheus.yml              
│   └── grafana_dashboard.json      
│
├── data/                           
│   ├── raw/
│   ├── processed/
│   └── models/
│
├── notebooks/                      
│   ├── 01_ingestion.ipynb
│   ├── 02_preparation.ipynb
│   ├── 03_training.ipynb
│   └── 04_visualization.ipynb
│
├── src/
│   ├── ingestion/
│   │   ├── api_ingestion.py
│   │   ├── kafka_stream.py
│   │   ├── sql_ingestion.py
│   │   └── mongo_ingestion.py
│   │
│   ├── preparation/
│   │   ├── cleaning.py
│   │   ├── feature_engineering.py
│   │   └── validation.py
│   │
│   ├── ml/
│   │   ├── train_model.py
│   │   ├── validate_model.py
│   │   ├── register_model.py
│   │   └── mlflow_tracking.py
│   │
│   ├── deployment/
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── routers/
│   │   │   │   ├── predict.py
│   │   │   │   └── health.py
│   │   │   └── models_loader.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── orchestration/
│   │   ├── airflow_dags/
│   │   │   └── data_pipeline_dag.py
│   │   ├── prefect_flows/
│   │   │   └── dataflow360_flow.py
│   │   └── scheduler_utils.py
│   │
│   ├── monitoring/
│   │   ├── metrics_collector.py
│   │   ├── drift_detection.py
│   │   ├── prometheus/
│   │   │   └── prometheus.yml
│   │   └── grafana/
│   │       └── dashboard.json
│   │
│   └── visualization/
│       ├── streamlit_app.py
│       └── dashboard_powerbi.pbix
│
├── docker/
│   ├── Dockerfile.api              # Image FastAPI
│   ├── Dockerfile.mlflow           # Image MLflow
│   ├── Dockerfile.airflow          # Image Airflow (scheduler + webserver)
│   ├── Dockerfile.prefect          # Option : si tu préfères Prefect à Airflow
│   ├── Dockerfile.kafka            # Kafka broker
│   ├── Dockerfile.postgres         # Base de données PostgreSQL
│   └── Dockerfile.grafana          # Monitoring
│
├── docker-compose.yml              # ⚙️ Orchestration de tous les services
│
├── mlflow/
│   ├── mlruns/
│   └── registry/
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_preparation.py
│   ├── test_models.py
│   └── test_api.py
│
├── scripts/
│   ├── build_all.sh
│   ├── start_all.sh
│   ├── stop_all.sh
│   ├── deploy_ci_cd.sh
│   └── reset_environment.sh
│
├── .github/
│   └── workflows/
│       ├── test_pipeline.yml
│       ├── deploy_api.yml
│       └── retrain_model.yml
│
├── requirements.txt
├── README.md
└── LICENSE

from prefect import flow, task
import subprocess

@task
def produce_data():
    subprocess.Popen(["python", "kafka_producer.py"])

@task
def consume_data():
    subprocess.Popen(["python", "kafka_consumer_to_hdfs.py"])

@flow(name="Real_Time_CSV_to_HDFS")
def main_flow():
    consume_data.submit()
    produce_data.submit()
    print("Producteur et consommateur lancés en parallèle.")

if __name__ == "__main__":
    main_flow()

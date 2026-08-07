# IoT Data Stream Processing & Imputation Pipeline

## Overview
This Capstone project focuses on building a High-Performance Computing (HPC) data streaming pipeline for IoT environments. The core research focus is **Data Stream Imputation** — handling and predicting missing sensor data in real-time.

## Architecture
- **IoT Data Producer**: A Python-based simulator generating IoT sensor data (with simulated missing values for imputation).
- **Message Queue (Ingestion)**: Apache Kafka.
- **Stream Processing Engine (HPC)**: Apache Flink (Handles the distributed computing and Data Imputation logic).
- **Time-series Database**: InfluxDB v2.
- **Deployment & Orchestration**: Kubernetes (K8s) & Helm.
- **Container Registry**: Harbor.

## Project Structure
- `producer.py`: Python script for IoT data generation.
- `Dockerfile`: Containerizes the IoT producer.
- `docker-compose.yml`: Local testing environment setup (Kafka, Flink Cluster, InfluxDB).
- `charts/iot-producer/`: Helm chart for deploying the producer to a Kubernetes cluster.
- `.github/workflows/ci.yml`: GitHub Actions CI/CD pipeline for building and pushing the Docker image to a Harbor registry.

## Local Development (Docker Compose)
To run the entire pipeline locally for testing purposes:
```bash
docker-compose up -d --build
```
This will start Zookeeper, Kafka, Flink (1 JobManager + 2 TaskManagers), InfluxDB, and the IoT Producer container.

To view the producer logs and see the generated data:
```bash
docker logs -f hpc-iot-producer
```

## Kubernetes Deployment (Helm)
For enterprise-level deployment, the project utilizes Helm charts to deploy onto a Kubernetes cluster.

To deploy the IoT Producer:
```bash
helm install hpc-iot-producer ./charts/iot-producer
```
You can override default values (like image tag or Kafka broker URL) using `--set`:
```bash
helm install hpc-iot-producer ./charts/iot-producer \
  --set image.tag="v1.0.0" \
  --set kafka.broker="kafka-service:9092"
```

## CI/CD Pipeline (GitHub Actions)
The project is equipped with an automated CI/CD pipeline. 
Upon pushing to the `main` branch, the pipeline automatically:
1. Logs into the Harbor Registry.
2. Builds the Docker image.
3. Pushes the image to Harbor with `latest` and commit SHA tags.

**Required GitHub Secrets:**
To enable the pipeline, configure the following secrets in your repository settings:
- `HARBOR_URL`: Your Harbor registry URL (e.g., `harbor.your-domain.com`)
- `HARBOR_USERNAME`: Harbor login username.
- `HARBOR_PASSWORD`: Harbor login password or CLI secret.

docker exec -it hpc-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hpc-raw-metrics --from-beginning

docker exec -it hpc-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hpc-raw-metrics

**Create the topic in Kafka**
docker exec -it hpc-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic hpc-raw-metrics --partitions 1 --replication-factor 1 --if-not-exists

**To test the Telegraf Message**
PS C:\Telegraf\telegraf-1.39.2_windows_amd64\telegraf-1.39.2> .\telegraf.exe --test --config telegraf.conf

**To Debug the Telegraf**
.\telegraf.exe --debug --config telegraf.conf

**Watch the number of records of message in Kafka**
docker exec -it hpc-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic hpc-raw-metrics


**Watch the message of Telegraf in the Kafka Topic**
docker exec -it hpc-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hpc-raw-metrics --partition 0 --offset 0

**The message format of Telegraf**

{"fields":{"bytes_recv":6927676,"bytes_sent":3547539,"drop_in":0,"drop_out":0,"err_in":0,"err_out":0,"packets_recv":12005,"packets_sent":11259,"speed":-1},"name":"net","tags":{"cluster":"hpc-local-poc","host":"LAPTOP-GOMEDOVR","interface":"Wi-Fi","node_name":"windows-client-1"},"timestamp":1785671380000}

## Testing Mock Data Ingestion

To run and test the python mock producer and Spark consumer locally:

1. **Start the Producer**:
```bash
python ingestion/producer.py
```
This will continuously generate and send mock HPC metrics (procstat, hw_power, win_eventlog) to the `hpc-raw-metrics` Kafka topic.

2. **Start the Spark Consumer**:
```bash
python ingestion/spark_consumer.py
```
This will read the stream from Kafka and print the parsed metrics to the console.

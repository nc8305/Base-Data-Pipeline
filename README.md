# HPC Data Stream Processing & Monitoring Pipeline

## Overview
This Capstone project focuses on building a robust High-Performance Computing (HPC) data streaming pipeline for IoT and telemetry environments. The system handles end-to-end data ingestion, real-time alert generation, stream imputation, and data visualization.

## Architecture
- **Telemetry Collector**: Telegraf (Collects system metrics from HPC nodes).
- **IoT Data Producer**: A Python-based simulator generating mock HPC sensor data (`procstat`, `hw_power`, `win_eventlog`).
- **Message Queue (Ingestion)**: Apache Kafka.
- **Real-time Stream Processing**: Apache Flink (PyFlink) - handles sliding windows, aggregation, and anomaly alerting.
- **Near-Realtime Processing**: Apache Spark (PySpark) - parses and processes raw data streams.
- **Time-series OLAP Database**: ClickHouse.
- **Data Lakehouse Storage**: MinIO.
- **Data Visualization**: Grafana.

## Project Structure
- `ingestion/`: Contains the Telegraf configuration and the Python mock data producer.
- `stream_processing/`: Contains the PyFlink DataStream jobs (e.g., `realtime_alert.py`).
- `test_cases/`: Testing utilities (Mock Kafka consumers, jar download scripts).
- `docker-compose.yml`: Local testing environment setup for all infrastructure components.
- `charts/`: Helm charts for Kubernetes deployment.
- `Dockerfile`: Containerizes the Python mock producer.

---

## 1. Prerequisites
- **Docker Desktop** (with WSL2 integration enabled on Windows).
- **Python 3.10**: PyFlink 1.18.1 strictly requires Python 3.10 maximum. Do not use Python 3.11 or 3.12.
- **Java 11**: Required for running the PyFlink MiniCluster locally.

---

## 2. Infrastructure Setup (Docker Compose)
To spin up the entire infrastructure (Kafka, Flink Cluster, ClickHouse, MinIO, Grafana, Telegraf):
```bash
docker-compose up -d
```

---

## 3. Real-time Alerting Pipeline (PyFlink)

We have built a real-time alerting pipeline that calculates a **10-second tumbling window** over CPU metrics to avoid alert fatigue (spam alerts) common in HPC environments.

### Testing the Pipeline
To test the pipeline end-to-end, follow these steps:

**Step 3.1: Prepare the Python 3.10 Environment**
If you are on Ubuntu 24.04 (WSL), you need to install Python 3.10:
```bash
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.10 python3.10-venv -y
python3.10 -m venv .venv_flink
source .venv_flink/bin/activate
pip install apache-flink==1.18.1 kafka-python
```

**Step 3.2: Download Flink Kafka Connector**
```bash
python test_cases/download_jar.py
```

**Step 3.3: Run the Pipeline (Open 3 Terminals)**
Make sure you run `source .venv_flink/bin/activate` in every terminal.

1. **Start the Alert Consumer (Terminal 1)**: Listens for critical anomalies.
```bash
python test_cases/test_consumer.py
```
2. **Start the PyFlink Job (Terminal 2)**: Runs the windowing and aggregation engine.
```bash
python stream_processing/realtime_alert.py
```
3. **Start the Mock Producer (Terminal 3)**: Pumps fake `procstat` CPU data into Kafka.
```bash
python ingestion/producer.py
```
*If the CPU stays above 90% for 10 seconds, Terminal 1 will trigger a `HIGH_CPU_AVERAGE_10_SECS` JSON alert.*

---

## 4. Near-Realtime Ingestion (Spark)
To run the PySpark consumer that parses the raw Telegraf schema and prints it to the console:
```bash
python ingestion/spark_consumer.py
```

---

## 5. Kubernetes Deployment (Helm)
For enterprise-level deployment onto a Kubernetes cluster:
```bash
helm install hpc-iot-producer ./charts/iot-producer
```
Override default values using `--set`:
```bash
helm install hpc-iot-producer ./charts/iot-producer \
  --set image.tag="v1.0.0" \
  --set kafka.broker="kafka-service:9092"
```

---

## 6. CI/CD Pipeline (GitHub Actions)
Upon pushing to the `main` branch, the pipeline automatically:
1. Logs into the Harbor Registry.
2. Builds the Docker image.
3. Pushes the image to Harbor with `latest` and commit SHA tags.

**Required GitHub Secrets:**
- `HARBOR_URL`, `HARBOR_USERNAME`, `HARBOR_PASSWORD`

---

## Appendix: Useful Kafka & Telegraf Commands

**Check Kafka Topics:**
```bash
docker exec -it hpc-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Consume Raw Metrics from Kafka:**
```bash
docker exec -it hpc-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hpc-raw-metrics
```

**Test Telegraf Configuration (Windows):**
```powershell
.\telegraf.exe --test --config telegraf.conf
.\telegraf.exe --debug --config telegraf.conf
```

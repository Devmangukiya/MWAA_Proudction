# MWAA Production Log Pipeline

## 🚀 Overview

This project implements a robust data pipeline for simulating, transporting, and indexing website log data at scale. It leverages Apache Airflow, Kafka, and Elasticsearch, orchestrated for automated extraction and searchability of web server logs in a cloud-friendly environment.

## 🎯 Problem Statement

Organizations need scalable solutions for generating, aggregating, and analyzing high-volume synthetic or real server logs, enabling near-real-time search, monitoring, and troubleshooting using modern cloud-based tools.

## 🧠 Solution Approach

- Synthetic log entries are generated and published to a Kafka topic in bulk, simulating high-traffic web environments.
- An Airflow DAG orchestrates the periodic creation and consumption of logs.
- Kafka acts as the transport layer for log messages.
- A consumer pipeline reads these logs, parses and normalizes them, and then indexes them into an Elasticsearch cluster, making them searchable.
- The entire system leverages secrets management via AWS Secrets Manager for secure configuration.

## ⚙️ Features

- **Synthetic Log Generation**: Bulk generation of realistic HTTP access logs using the Faker library.
- **Scalable Log Transport**: Uses Kafka for robust and scalable log streaming.
- **Automated Orchestration**: Powered by Apache Airflow DAGs to schedule and monitor tasks.
- **Production-Grade Security**: Fetches credentials securely from AWS Secrets Manager.
- **Bulk Indexing**: Efficiently batches log indexing into Elasticsearch.
- **Fault Tolerance**: Includes robust error handling, task retries, and logging.

## 🏗️ Architecture / Workflow

1. **Log Producer DAG**
   - Scheduled every 5 minutes via Airflow.
   - Generates 15,000 synthetic logs per run.
   - Publishes logs to a Kafka topic (`billion_website_logs`).

2. **Log Consumer Pipeline**
   - Consumes batches of logs from the Kafka topic.
   - Parses and normalizes log fields (IP, timestamp, endpoint, etc.).
   - Indexes logs into an Elasticsearch index (`billion_website_logs`).
   - Retries and logs failures; closes resources safely.

**Pseudo-Diagram:**

```
[Airflow DAGs]
    |            |
[Producer]   [Consumer]
    |            |
  Kafka <--------|
    |
Elasticsearch
```

## 🛠️ Tech Stack

- **Python**
- **Apache Airflow**
- **Kafka (confluent-kafka)**
- **Elasticsearch (python client)**
- **Faker (for synthetic logs)**
- **AWS Secrets Manager (boto3)**
- **Docker** (suggested for production setup)

## 📦 Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Devmangukiya/MWAA_Proudction.git
   cd MWAA_Proudction
   ```

2. **Set up Python Environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Requirements**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Secrets in AWS**
   - Populate AWS Secrets Manager with required keys under `MWAA_Secrets_V2` (see below).

5. **Set up Kafka and Elasticsearch**
   - Provision Kafka (cloud or local cluster).
   - Deploy Elasticsearch (cloud or self-hosted).

## ▶️ Usage

1. **Run Airflow Scheduler and Webserver**

   Ensure Airflow is installed and initialized (see Airflow documentation).

   ```bash
   airflow db init
   airflow users create ...   # Create an admin user
   airflow webserver --port 8080
   airflow scheduler
   ```

2. **Deploy DAGs**
   - Place all files in the `dags/` directory into your Airflow DAGs folder or mount the `dags` directory when running Airflow in Docker/MWAA.

3. **Trigger the Log Generation and Processing**
   - The DAGs will run automatically as per their schedule.
   - Or, trigger manually in the Airflow UI.

## 📁 Project Structure

```
MWAA_Proudction/
├── dags/
│   ├── logs_processing_pipeline.py  # Kafka-to-Elastic consumer logic in Airflow DAG
│   └── logs_producer.py             # Log generator and Kafka producer as Airflow DAG
├── requirements.txt                 # Python dependencies
├── .gitignore
└── .github/                         # (optional) GitHub metadata/workflows
```

- `logs_producer.py`: Airflow DAG that generates and sends synthetic logs to Kafka.
- `logs_processing_pipeline.py`: Consumes logs from Kafka, parses & indexes them in Elasticsearch.
- `requirements.txt`: Required Python packages.
- `.github/`: GitHub-specific configurations (e.g., CI/CD).

## 📊 Results / Output

- **Primary output**: Searchable, structured access logs in an Elasticsearch index (`billion_website_logs`).
- **Monitoring**: Airflow logs and Elasticsearch/Kibana can be used to inspect pipeline health and indexed data.

## 🔮 Future Improvements

- Support for multiple log formats and sources.
- Real-time anomaly detection or alerting on logs.
- Integration with monitoring tools (Grafana, Kibana dashboards).
- Containerized deployment (Docker Compose).
- Auto-scaling for log producers/consumers.

## 🤝 Contributing

1. Fork the repository.
2. Create a new branch for your feature/fix.
3. Commit and push your changes.
4. Open a pull request.

## 📜 License

This project is protected under the repository's specified license (see LICENSE file if present).

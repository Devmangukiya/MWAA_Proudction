from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from confluent_kafka import Producer
from faker import Faker
import logging
from datetime import datetime, timedelta
import boto3
import json
import random
from dags.utils import get_secret

fake = Faker()
logger = logging.getLogger(__name__)

def get_secret(secret_name,region = 'us-east-2'):
    """Fetch secrets from AWS Secrets Manager"""

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"Secret retrieval Failed. {e}")
        raise

def create_kafka_producer(config):
    return Producer(config)

def generate_log():
    """Generate synthetic log entry using Faker library"""
    methods = ['GET','POST','PUT','DELETE']
    endpoints = ['/api/users', '/home', '/about', '/contact', '/services']
    statuses = [200, 301, 302, 400, 404, 500]

    user_agents = [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)',
        'Mozilla/5.0 (X11; Linux x86_64)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]

    referrers = ['https://example.com','https://google.com','-','https://bing.com','https://yahoo.com']
    ip = fake.ipv4()
    timestamp = datetime.now().strftime('%b %d %Y, %H:%M:%S')
    method = random.choice(methods)
    endpoints = random.choice(endpoints)
    status = random.choice(statuses)
    size = random.randint(1000, 50000)
    referrers = random.choice(referrers)
    user_agents = random.choice(user_agents)

    log_entry = (
        f'{ip} -- [{timestamp}] \ "{method} {endpoints} HTTP/1.1" {status} {size} "{referrers}" {user_agents}'
    )

    return log_entry

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def produce_logs(**context):
    """Prodeuce log entries into kafka"""

    secrets = get_secret('MWAA_Secrets_V2')
    kafka_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'session.timeout.ms': 50000
    }
    
    producer = create_kafka_producer(kafka_config)
    topic = 'billion_website_logs'

    for _ in range(15000):
        log = generate_log()
        try:
            producer.produce(topic,log.encode('UTF-8'),on_delivery=delivery_report)
            producer.flush()
        except Exception as e:
            logger.error(f"Failed to produce log entry: {e}")
            raise

    logger.info(f"Successfully produced 15000 log entries to topic {topic}")


default_args = {
    'owner': 'Data Mastry Lab',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'log_generation_pipeline',
    default_args=default_args,
    description='A DAG to generate and send log messages to Kafka',
    schedule_interval = '*/5 * * * *',
    start_date = datetime(2026,2,16),
    catchup=False,
    tags= ['log','kafka','production']
)

produce_logs_task = PythonOperator(
    task_id = 'generate_and_produce_logs',
    python_callable = produce_logs,
    dag = dag
)

produce_logs()
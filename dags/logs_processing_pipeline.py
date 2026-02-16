from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch, helpers
import json
import logging
import re
import boto3

logger = logging.getLogger(__name__)

def get_secret(secret_name,region = 'us-east-1'):
    """Fetch secrets from AWS Secrets Manager"""

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"Secret retrieval Failed. {e}")
        raise

def parse_log_entry(log_entry):
    log_pattern = (
        r'(?P<ip>\d+\.\d+\.\d+\.\d+) -- '
        r'\[(?P<timestamp>.*?)\] '
        r'"(?P<method>\w+) (?P<endpoint>.*?) (?P<protocol>.*?)" '
        r'(?P<status>\d+) (?P<size>\d+) '
        r'"(?P<referrer>.*?)" (?P<user_agent>.*)'
    )

    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f"Failed to parse log entry: {log_entry}")

    data = match.groupdict()

    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], '%b %d %Y, %H:%M:%S')
        data['@timestamp'] = parsed_timestamp.isoformat()
    except ValueError:
        logger.error(f"timestamp parsing error: {data['timestamp']}")
        return None
    
    return data
    
def consume_and_index_logs(**context):
    secrets = get_secret('MWAA_Secrets_V2')

    consumer_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'group.id': 'mwaa_log_indexer',
        'auto.offset.reset': 'latest'
    }

    es_config = {
        'hosts': [secrets['ELASTICSEARCH_URL']],
        'api_key': secrets['ELASTICSEARCH_API_KEY']
    }

    consumer = Consumer(consumer_config)
    es = Elasticsearch(**es_config)

    topic = 'billion_website_logs'
    consumer.subscribe([topic])

    try:
        index_name = "billion_website_logs"
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logger.info(f"Created Elasticsearch index: {index_name}")
    except Exception as e:
        logger.error(f"Failed to create elasticsearch index: {index_name} {e}")

    try:
        logs = []
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                break

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())
            
            log_entry = msg.value().decode('utf-8')
            parsed_log = parse_log_entry(log_entry)

            if parsed_log:
                logs.append(parsed_log)

            # index when 15000 logs are collected.
            if len(logs) >= 15000:
                actions = [
                    {
                        '_op_type': 'create',
                        '_index': index_name,
                        '_source': log
                    }
                    for log in logs
                ]

                success,failed = helpers.bulk(es,actions,refresh=True)
                logger.info(f"Indexd {success} logs, {len(failed)} Failed")
                logs = []


    except Exception as e:
        logger.error(f"Failed to index log: {e}")

    # index any remaining logs
    try:
        if logs:
            actions = [
                {
                    '_op_type': 'create',
                    '_index': index_name,
                    '_source': log
                }
                for log in logs
            ]

            helpers.bulk(es,actions,refresh=True)

    except Exception as e:
        logger.error(f"Log processing error : {e}")
        
    finally:
        consumer.close()
        es.close()


default_args = {
    'owner': 'Data Mastry Lab',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# dag = DAG(
#     'log_consumer_pipeline',
#     default_args=default_args,
#     description='Consume and index synthetic logs',
#     schedule_interval = '*/5 * * * *',  
#     strart_date = datetime(2026,2,16),
#     catchup=False,
#     tags= ['log','kafka','production']
# )

# consume_logs_task = PythonOperator(
#     task_id = 'generate_and_consume_logs',
#     python_callable=consume_and_index_logs,
#     dag = dag
# )

consume_and_index_logs()
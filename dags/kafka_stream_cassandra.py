import uuid
from datetime import datetime
import json
import logging
import time
import requests
from kafka import KafkaProducer, KafkaConsumer
from cassandra.cluster import Cluster
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'rjacaac',
    'start_date': datetime(2024, 9, 6, 11, 0)
}

def get_data():
    response = requests.get("https://randomuser.me/api/")
    return response.json()['results'][0]

def format_data(res):
    location = res['location']
    return {
        'id': str(uuid.uuid4()),
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        'post_code': str(location['postcode']),
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    end_time = time.time() + 30  # Run for 30 seconds

    while time.time() < end_time:
        try:
            res = get_data()
            formatted_data = format_data(res)
            producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))
            logging.info(f"Sent data to Kafka: {formatted_data}")
        except Exception as e:
            logging.error(f'An error occurred: {e}')

    producer.close()

def connect_to_cassandra():
    cluster = Cluster(['cassandra'])  # Use the service name defined in docker-compose
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS users_data_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS users_data_streams.users_data (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TIMESTAMP,
        registered_date TIMESTAMP,
        phone TEXT,
        picture TEXT
    )
    """)

    return session

def insert_into_cassandra(session, data):
    try:
        user_id = uuid.UUID(data['id'])
        dob = datetime.fromisoformat(data['dob'].replace('Z', '+00:00'))
        registered_date = datetime.fromisoformat(data['registered_date'].replace('Z', '+00:00'))

        query = """
        INSERT INTO users_data_streams.users_data (id, first_name, last_name, gender, address, post_code, email,
                                                username, dob, registered_date, phone, picture)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        session.execute(query, (
            user_id, data['first_name'], data['last_name'], data['gender'], data['address'], data['post_code'],
            data['email'], data['username'], dob, registered_date, data['phone'], data['picture']
        ))

        logging.info(f"Data inserted for {data['first_name']} {data['last_name']}")
    except Exception as e:
        logging.error(f"Failed to insert data into Cassandra: {e}")

def consume_kafka(timeout=130):  # Timeout of 30 seconds
    consumer = KafkaConsumer(
        'users_created',
        bootstrap_servers=['broker:29092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    cassandra_session = connect_to_cassandra()
    start_time = time.time()

    for message in consumer:
        logging.info(f"Received message: {message.value}")
        insert_into_cassandra(cassandra_session, message.value)

        # Check for timeout
        if time.time() - start_time > timeout:
            logging.info("Timeout reached, stopping Kafka consumer.")
            break

    consumer.close()
    cassandra_session.shutdown()



with DAG('kafka_stream_cassandra',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

    consume_task = PythonOperator(
        task_id='consume_and_store_data',
        python_callable=consume_kafka
    )

    streaming_task >> consume_task
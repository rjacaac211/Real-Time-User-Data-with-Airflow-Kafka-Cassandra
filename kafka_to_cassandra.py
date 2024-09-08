import json
import logging
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from datetime import datetime
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)


# Connect to the Cassandra cluster
def connect_to_cassandra():
    cluster = Cluster(['localhost'])
    session = cluster.connect()

    # Create keyspace if not exists
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    # Drop table if exists
    session.execute("""
    DROP TABLE IF EXISTS spark_streams.created_users;
    """)

    # Create table with the correct schema
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TIMESTAMP,  -- Add the dob field
        registered_date TIMESTAMP,
        phone TEXT,
        picture TEXT
    )
    """)

    return session


# Insert the data into Cassandra
def insert_into_cassandra(session, data):
    try:
        # Convert the id to UUID
        user_id = uuid.UUID(data['id'])

        # Parse date fields to datetime objects
        dob = datetime.fromisoformat(data['dob'].replace('Z', '+00:00'))
        registered_date = datetime.fromisoformat(data['registered_date'].replace('Z', '+00:00'))

        # Convert post_code to string
        post_code = str(data['post_code'])

        # Log data to check correctness
        logging.info(f"Inserting data: {data}")

        # Insert into Cassandra
        query = """
        INSERT INTO spark_streams.created_users (id, first_name, last_name, gender, address, post_code, email,
                                                username, dob, registered_date, phone, picture)
        VALUES (%(id)s, %(first_name)s, %(last_name)s, %(gender)s, %(address)s, %(post_code)s, %(email)s,
                %(username)s, %(dob)s, %(registered_date)s, %(phone)s, %(picture)s)
        """

        session.execute(query, {
            'id': user_id,
            'first_name': data['first_name'],
            'last_name': data['last_name'],
            'gender': data['gender'],
            'address': data['address'],
            'post_code': post_code,
            'email': data['email'],
            'username': data['username'],
            'dob': dob,
            'registered_date': registered_date,
            'phone': data['phone'],
            'picture': data['picture']
        })

        logging.info(f"Data inserted for {data['first_name']} {data['last_name']}")
    except Exception as e:
        logging.error(f"Failed to insert data into Cassandra: {e}")



# Kafka consumer function
def consume_kafka():
    consumer = KafkaConsumer(
        'users_created',  # Kafka topic name
        bootstrap_servers=['localhost:9092'],  # Adjust to your Kafka broker
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    cassandra_session = connect_to_cassandra()

    for message in consumer:
        logging.info(f"Received message: {message.value}")
        insert_into_cassandra(cassandra_session, message.value)


if __name__ == "__main__":
    consume_kafka()

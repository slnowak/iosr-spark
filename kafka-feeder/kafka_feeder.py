from kafka import KafkaProducer


def read_flights_file(filename):
    with open(filename) as f:
        next(f)
        for line in f:
            yield line


def feed_kafka(flight_lines, kafka_servers, kafka_topic):
    producer = KafkaProducer(bootstrap_servers=kafka_servers, api_version=(0, 10))
    for flight_line in flight_lines:
        producer.send(kafka_topic, flight_line)
    producer.flush()


if __name__ == '__main__':
    DEFAULT_KAFKA_TOPIC_NAME = 'flights'
    DEFAULT_DATA_FILE = 'sample'
    DEFAULT_KAFKA_SERVERS = '192.168.1.106:9092'

    flights = read_flights_file(DEFAULT_DATA_FILE)
    feed_kafka(flights, DEFAULT_KAFKA_SERVERS, DEFAULT_KAFKA_TOPIC_NAME)

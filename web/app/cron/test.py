
print("test cron job -----------")
from kafka import KafkaProducer, KafkaConsumer
BOOTSTRAP_SERVER = "172.22.0.3"
TOPIC_NAME = "test_kafka"

producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])

producer.send(TOPIC_NAME, b'push-from-cron')
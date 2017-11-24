import logging
from kafka import KafkaConsumer
TOPIC_NAME = "test"
BOOTSTRAP_SERVER = '172.17.0.1'
logging.basicConfig(level=logging.DEBUG)

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[BOOTSTRAP_SERVER])
logging.info(consumer)
logging.critical("----------consumer-------")



logging.critical("----------end consuming-------")

for msg in consumer:
    logging.info("--------------------------a")
    logging.info(msg.value)
    print(msg)
metrics = consumer.metrics()
logging.info(metrics)
consumer.close()
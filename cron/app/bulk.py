import logging
from kafka import KafkaProducer
logging.basicConfig(level=logging.DEBUG)


BOOTSTRAP_SERVER = "172.17.0.1"
producer =  KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])
for _ in range(100):

    producer.send('test', b'raw_bytes')
metrics = producer.metrics()
logging.info(metrics)
logging.critical("----------end processing-------")
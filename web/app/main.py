from flask import Flask, jsonify, render_template
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory
from uwsgidecorators import postfork
#from kafka import KafkaProducer, KafkaConsumer
# from .settings import BOOTSTRAP_SERVER, TOPIC_NAME
import logging
import os
from datetime import datetime
logging.basicConfig(level=logging.DEBUG)


BOOTSTRAP_SERVER = "172.22.0.3"
TOPIC_NAME = "test_kafka"
app = Flask(__name__,  static_folder="../static/dist", template_folder="../static")

tasks = [
  {
    'id': 1,
    'title': u'Buy groceries',
    'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
    'done': False
  },
  {
    'id': 2,
    'title': u'Learn Python',
    'description': u'Need to find a good Python tutorial on the web',
    'done': False
  }
]


# @app.before_request
# def before_request():
#   session = cluster.connect('test1')
session = None
prepared = None
@postfork
def connect():
  global session, prepared, cluster
  try:
    cluster = Cluster(['127.0.0.1'], port=int(os.environ.get('CASSANDRA_PORT_9042_TCP_PORT', 9042)))
    session = cluster.connect()
  except Exception as error:
    cluster = None
    session = None
    print(error)
  print("hogeege")


@app.route("/")
def hello():
  print("hoasdasdas")

  return render_template("index.html")
  #return "Hey I'm using Docker!"

@app.route("/producer", methods=["GET"])
def producer():
  producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])

  producer.send(TOPIC_NAME, b'test-result-is')
  return "producer"
  #return "Hey I'm using Docker!"

@app.route("/test", methods=['GET'])
def test():
  print("hoge")

  # consumer = KafkaConsumer(
  #   bootstrap_servers=BOOTSTRAP_SERVER,
  #   auto_offset_reset = 'earliest', # start at earliest topic
  #   group_id = None # do no offest commit
  # )
  consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[BOOTSTRAP_SERVER])

  print("consumer--is coming")
  for msg in consumer:
    print("--------------------------a")
    print(msg)
  consumer.close()

  return "test"

@app.route('/cassandra', methods=['GET'])
def cassandra():

  session.row_factory = ordered_dict_factory
  rows = session.execute('SELECT * FROM system.schema_keyspaces LIMIT 10')

  return jsonify(data=rows, hostname=os.uname()[1],
                 current_time=str(datetime.now()))
  # cluster = Cluster()
  # session = cluster.connect('test1')
  # print(session)
  # print("cassandra---------------")
  # return "cassandra"
@app.route('/todo/api/v1.0/tasks', methods=['GET'])
def get_tasks():
  return jsonify({'tasks': tasks})

# @app.route('/<path:path>')
# def route_frontend(path):
#   # ...could be a static file needed by the front end that
#   # doesn't use the `static` path (like in `<script src="bundle.js">`)
#   file_path = os.path.join(app.static_folder, path)
#   if os.path.isfile(file_path):
#     return send_file(file_path)
#   # ...or should be handled by the SPA's "router" in front end
#   else:
#     index_path = os.path.join(app.static_folder, 'index.html')
#     return send_file(index_path)


if __name__ == "__main__":
  app.run(host="0.0.0.0", debug=True, port=80)

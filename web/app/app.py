import os
import datetime
from flask import Flask, jsonify, redirect, url_for, Response, send_file
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory, dict_factory
from kafka import KafkaProducer
import json
import logging
app = Flask(__name__)
KEYSPACE_NAME = "weather_forecast"
TABLE_NAME = "prediction"
BOOTSTRAP_SERVER = "172.17.0.1"
TOPIC_NAME = "batch_processing"


logging.basicConfig(level=logging.DEBUG)



@app.route('/')
def index():
    index_path = os.path.join(app.static_folder, 'index.html')
    return send_file(index_path)


@app.route("/api/v1/predictions", methods=['GET'])
def predictions_index():
    #global session
    cluster = Cluster([os.environ.get('CASSANDRA_PORT_9042_TCP_ADDR', 'localhost')],
                      port=int(os.environ.get('CASSANDRA_PORT_9042_TCP_PORT', 9042))
                      )
    session = cluster.connect(KEYSPACE_NAME)
    session.row_factory = dict_factory

    sql = """
        SELECT city, condition, prediction_percent, rain_3h, snow_3h
        FROM prediction
        --WHERE city = 'san francisco'
        --WHERE city = %s
        -- AND predicted_at > %s
        -- ALLOW FILTERING
    """

    #today_timestamp = int(float(datetime.date.today().strftime("%s.%f"))) * 1000

    city = 'san francisco'
    forecast_data = session.execute(sql)
    logging.critical(forecast_data)

    forecast = list(forecast_data)[0]
    logging.critical(forecast)

    return json.dumps(forecast)

@app.route("/api/v1/predictions", methods=['POST'])
def predictions_create():
    try:
        kafka = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])
        kafka.send(TOPIC_NAME, b'bulk_processing')
        logging.info("send bulk_processing event!!")
        return Response(status=201, mimetype='application/json')
    except Exception as error:
        logging.error(error)
        return Response(error.message, status=400, mimetype='application/json')



if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
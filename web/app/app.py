import os
import datetime
from flask import Flask, jsonify, redirect, url_for, Response, send_file
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory, dict_factory
from kafka import KafkaProducer
import json
import logging
app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['TEMPLATES_AUTO_RELOAD'] = True
KEYSPACE_NAME = "weather_forecast"
TABLE_NAME = "prediction"
BOOTSTRAP_SERVER = "172.17.0.1"
TOPIC_NAME = "batch_data"


logging.basicConfig(level=logging.DEBUG)

@app.after_request
def add_header(response):
    # response.cache_control.no_store = True
    if 'Cache-Control' not in response.headers:
        response.headers['Cache-Control'] = 'no-store'
    return response


@app.route('/')
def index():
    index_path = os.path.join(app.static_folder, 'index.html')
    return send_file(index_path)


@app.route("/api/v1/predictions", methods=['GET'])
def predictions_index():
    #global session
    logging.critical("predictions loading!!!")
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

    forecast = list(forecast_data)
    return jsonify(predictions=forecast)

@app.route("/api/v1/predictions", methods=['POST'])
def predictions_create():
    logging.critical("predictions create called!!")
    try:
        kafka = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])
        kafka.send(TOPIC_NAME, b'bulk_processing')
        logging.info("send bulk_processing event!!")
        return Response(status=201, mimetype='application/json')
    except Exception as error:
        logging.error(error)
        logging.error("predictions_create error!!!!!!!!!!!!!!")
        return Response(error.message, status=400, mimetype='application/json')



if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

import os
import sys
import datetime
from flask import Flask, jsonify, redirect, url_for, Response, send_file
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from kafka import KafkaProducer
import json
import logging
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/shared'))

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['TEMPLATES_AUTO_RELOAD'] = True
KEYSPACE_NAME = 'weather_forecast'
PREDICTION_TABLE_NAME = 'prediction'
BOOTSTRAP_SERVER = '172.17.0.1'
BATCH_DATA_TOPIC_NAME = 'batch'
SAN_FRANCISCO_CITY_NAME = 'san francisco'


@app.after_request
def add_header(response):
    if 'Cache-Control' not in response.headers:
        response.headers['Cache-Control'] = 'no-store'
    return response


@app.route('/')
def index():
    index_path = os.path.join(app.static_folder, 'index.html')
    return send_file(index_path)


@app.route("/api/v1/predictions", methods=['GET'])
def predictions_index():
    logging.critical("predictions loading!!!")
    cluster = Cluster([os.environ.get('CASSANDRA_PORT_9042_TCP_ADDR', 'localhost')],
                      port=int(os.environ.get('CASSANDRA_PORT_9042_TCP_PORT', 9042))
                      )
    logging.critical(cluster)
    logging.critical("app cluser loading-----")
    session = cluster.connect(KEYSPACE_NAME)

    logging.critical(session)

    session.row_factory = dict_factory
    forecast = None

    rows = session.execute('SELECT DISTINCT version, city_name FROM {}'.format(PREDICTION_TABLE_NAME))

    latest_version = 0
    for row in rows._current_rows:
        if latest_version < row['version']:
            latest_version = row['version']

    sql = """
        SELECT city_name, forecast_at, temperature
        FROM {}
        WHERE version = %s
        AND city_name = %s
        LIMIT 1
    """.format(PREDICTION_TABLE_NAME)

    #today_timestamp = int(float(datetime.date.today().strftime("%s.%f"))) * 1000

    forecast_data = session.execute(sql, (latest_version, SAN_FRANCISCO_CITY_NAME))
    logging.critical(forecast_data)
    forecast = list(forecast_data)

    return jsonify(predictions=forecast)

@app.route("/api/v1/predictions", methods=['POST'])
def predictions_create():
    try:
        kafka = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])
        kafka.send(BATCH_DATA_TOPIC_NAME, b'bulk_processing')
        kafka.flush(10)
        logging.critical("send bulk_processing event!!")
        return Response(status=201, mimetype='application/json')
    except Exception as error:
        logging.error(error)
        logging.error("predictions_create error!!!!!!!!!!!!!!")
        return Response(error.message, status=400, mimetype='application/json')



if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

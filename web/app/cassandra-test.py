from cassandra.cluster import Cluster
import logging
logging.basicConfig(level=logging.DEBUG)
cluster = Cluster(['172.22.0.2'])

session = cluster.connect('test1')
session.execute("CREATE TYPE address (street text, zipcode int)")


from cassandra.cluster import Cluster
import logging
logging.basicConfig(level=logging.DEBUG)
cluster = Cluster()

session = cluster.connect()
session.set_keyspace('test1')
session.execute("CREATE TYPE address (street text, zipcode int)")


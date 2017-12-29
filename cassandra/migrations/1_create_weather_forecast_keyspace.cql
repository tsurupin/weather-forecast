CREATE KEYSPACE weather_forecast
WITH replication =
{
  'class':'SimpleStrategy',
  'replication_factor':'1'
};

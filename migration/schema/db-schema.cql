CREATE KEYSPACE IF NOT EXISTS weather_forecast WITH replication =
{
  'class':'SimpleStrategy',
  'replication_factor':'1'
};

CREATE TABLE IF NOT EXISTS weather_forecast.raw_data
(
  id UUID,
  longitude float,
  latitude float,
  condition  text,
  condition_details text,
  temperature float,
  pressure int,
  humidity int,
  wind_speed float,
  wind_degree int,
  clouds_all int,
  rain_3h int,
  snow_3h int,
  measured_at timestamp,
  country varchar,
  sunrise timestamp,
  sunset timestamp,
  city varchar,
  PRIMARY KEY (id, city)
);

CREATE TABLE IF NOT EXISTS weather_forecast.prediction
(
  id UUID,
  condition  text,
  forecast_on date,
  rain_3h int,
  snow_3h int,
  predicted_at timestamp,
  prediction_percent float,
  city varchar,
  PRIMARY KEY (id, city)
);

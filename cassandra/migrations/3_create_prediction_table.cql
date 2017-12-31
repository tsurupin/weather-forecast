CREATE TABLE IF NOT EXISTS weather_forecast.prediction
(
  forecast_on date,
  predicted_at timestamp,
  temperature float,
  city_name varchar,
  version int,
  PRIMARY KEY ((version, city_name), forecast_on)
) WITH CLUSTERING ORDER BY (forecast_on DESC);

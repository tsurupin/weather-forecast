CREATE TABLE IF NOT EXISTS weather_forecast.prediction
(
  forecast_at timestamp,
  predicted_at timestamp,
  temperature float,
  city_name varchar,
  version int,
  PRIMARY KEY ((version, city_name), forecast_at)
) WITH CLUSTERING ORDER BY (forecast_at DESC);

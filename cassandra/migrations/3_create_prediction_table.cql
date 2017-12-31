CREATE TABLE IF NOT EXISTS weather_forecast.prediction
(
  id varchar,
  condition  text,
  forecast_on date,
  predicted_at timestamp,
  preticipation_percent float,
  city_name varchar,
  PRIMARY KEY ((id, city_name), predicted_at)
);

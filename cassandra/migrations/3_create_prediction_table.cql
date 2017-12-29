CREATE TABLE IF NOT EXISTS weather_forecast.prediction
(
  id bigint,
  condition  text,
  forecast_on date,
  predicted_at timestamp,
  preticipation_percent float,
  city varchar,
  PRIMARY KEY (id, city)
);

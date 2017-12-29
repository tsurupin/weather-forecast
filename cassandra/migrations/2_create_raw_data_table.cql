CREATE TABLE IF NOT EXISTS weather_forecast.raw_data
(
  id bigint,
  dt int,
  dt_iso timestamp,
  city_id int,
  city_name varchar,
  temperature float,
  temperature_min float,
  temperature_max float,
  pressure int,
  humidity int,
  wind_speed float,
  wind_degree int,
  rain_3h int,
  snow_3h int,
  clouds_all int,
  condition_id  int,
  condition  text,
  condition_details text,
  measured_at timestamp,
  PRIMARY KEY (id, city_id)
);

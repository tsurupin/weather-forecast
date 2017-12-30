CREATE TABLE IF NOT EXISTS weather_forecast.raw_data
(
  id varchar,
  dt int,
  dt_iso timestamp,
  city_id int,
  city_name varchar,
  temperature float,
  temperature_max float,
  temperature_min float,
  pressure int,
  humidity int,
  wind_degree int,
  wind_speed float,
  rain_3h float,
  snow_3h float,
  clouds_all float,
  condition_id  int,
  condition  text,
  condition_details text,
  measured_at timestamp,
  PRIMARY KEY (id, city_id)
);

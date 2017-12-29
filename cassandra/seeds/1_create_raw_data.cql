COPY weather_forecast.raw_data (id, city_id, city_name, clouds_all, condition, condition_details, condition_id, dt, dt_iso, humidity,pressure, rain_3h, snow_3h, temperature, temperature_max, temperature_min, wind_degree, wind_speed)
FROM '../usr/local/raw_data/sanfrancisco.csv'
WITH DELIMITER=',' AND HEADER=TRUE ;

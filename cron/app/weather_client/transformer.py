CITY_ID_TABLE = {
    'San Francisco': 5391959
}

class Transformer(object):
    def __init__(self, raw_data):
        self._raw_data = raw_data

    def run(self):
        data = {}

        for key, value in self._raw_data.items():

            if key == 'coord':
                data["longitude"] = value['lon']
                data["latitude"] = value['lat']
            if key == 'weather':
                data["condition_id"] = value[0]["id"]
                data["condition"] = value[0]["main"]
                data["condition_details"] = value[0]["description"]
            if key == "main":
                data["temperature"] = value["temp"]
                data["temperature_max"] = value["temp_max"]
                data["temperature_min"] = value["temp_min"]
                data["pressure"] = value["pressure"]
                data["humidity"] = value["humidity"]
            if key == "wind":
                data["wind_speed"] = value["speed"]
                data["wind_degree"] = value["deg"]
            if key == "clouds":
                data["clouds_all"] = value["all"]
            if key == "rain":
                data["rain_3h"] = value["3h"]
            if key == "snow":
                data["snow_3h"] = value["3h"]
            if key == "dt":
                data["dt"] = value
            if key == "sys":
                data["country_code"] = value["country"]
                data["sunrise"] = value["sunrise"]
                data["sunset"] = value["sunset"]
            if key == "name":
                data["city_name"] = value
                data['city_id'] = CITY_ID_TABLE[value]

        return data

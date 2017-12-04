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
                data["condition"] = value[0]["main"]
                data["condition_details"] = value[0]["description"]
            if key == "main":
                data["temperature"] = value["temp"]
                data["pressure"] = value["pressure"]
                data["humidity"] = value["humidity"]
            if key == "wind":
                data["wind_seepd"] = value["speed"]
                data["wind_degree"] = value["deg"]
            if key == "clouds":
                data["clouds_all"] = value["all"]
            if key == "rain":
                data["rain_3h"] = value["3h"]
            if key == "snow":
                data["snow_3h"] = value["3h"]
            if key == "dt":
                data["measured_at"] = value
            if key == "sys":
                data["country_code"] = value["country"]
                data["sunrise"] = value["sunrise"]
                data["sunset"] = value["sunset"]
            if key == "name":
                data["city"] = value

        return data

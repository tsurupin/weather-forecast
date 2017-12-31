import numpy as np
import pandas as pd
import datetime
import logging
from sklearn.decomposition import PCA
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import FeatureUnion

class DataFrameSelector(BaseEstimator, TransformerMixin):

    def __init__(self, attribute_names):
        self.attribute_names = attribute_names
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        return X[self.attribute_names].values

class FeatureTransformer(object):
    def __init__(self, data):
        self.data = data
        logging.critical(data)

    def perform(self):
        unused_columns = ['snow_1h', 'snow_24h', 'rain_24h', 'rain_1h', 'snow_3h', 'rain_today', 'snow_today', 'weather_icon', 'weather_id', 'sea_level', 'grnd_level', 'lat', 'lon', 'city_id', 'city_name']
        used_columns = set(self.data.columns) - set(unused_columns)
        self.hour_diffs =  [1,2,3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48]
        self.weather_description_columns = [
            'light intensity drizzle',
             'thunderstorm',
             'scattered clouds',
             'overcast clouds',
             'smoke',
             'mist',
             'heavy intensity drizzle',
             'moderate rain',
             'thunderstorm with heavy rain',
             'Sky is Clear',
             'very heavy rain',
             'light intensity shower rain',
             'broken clouds',
             'thunderstorm with light drizzle',
             'drizzle',
             'proximity thunderstorm with rain',
             'shower rain',
             'proximity thunderstorm with drizzle',
             'SQUALLS',
             'heavy snow',
             'haze',
             'heavy intensity rain',
             'few clouds',
             'thunderstorm with rain',
             'proximity thunderstorm',
             'light rain',
             'sky is clear',
             'light snow',
             'thunderstorm with light rain',
             'proximity shower rain',
             'fog'
        ]
        self.weather_main_columns = [
            'Thunderstorm',
             'Squall',
             'Snow',
             'Mist',
             'Rain',
             'Smoke',
             'Drizzle',
             'Clear',
             'Clouds',
             'Haze',
             'Fog'
         ]

        self.data = self._cleanup_features(self.data, used_columns)
        self.data, self.test_data = self._combine_previous_data(self.data)
        self.data = self._convert_target_to_int(self.data)
        target_data = self.data['target_temp']
        self.data = self.data.drop(['target_temp'], axis=1)
        self.data = self._transform_with_pipelines(self.data)
        self.test_data = self._transform_with_pipelines(self.test_data)
        return self.data, self.test_data


    def _cleanup_features(self, original_data, columns):
        data = original_data.copy()
        data = data.loc[:, columns]

        data['rain_3h'] = data['rain_3h'].fillna(0)
        data.drop_duplicates('dt', inplace=True)
        data = self._add_new_data(data)
        data = data.apply(self._transform_datetime, axis=1)
        unused_columns  = ['dt_iso', 'weather_main', 'weather_description', 'dt_datetime']
        data = data.drop(unused_columns, axis=1)
        data = data.reset_index(drop=True)
        return data

    def _combine_previous_data(self, original_data):
        data = original_data.copy()
        diff_columns = list(set(original_data.columns) - set(['dt', 'dt_iso', 'dt_datetime', 'target_temp', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 'month_8', 'month_9', 'month_10', 'month_11', 'month_12', 'year', 'dayofyear', 'dayofweek', 'hourofday', 'temp_min', 'temp_max'] + self.weather_description_columns))
        maximum_prev = 2 *  24
        for i in self.hour_diffs:
            for column in diff_columns:
                data['{}_{}_ago'.format(column, i)] = pd.Series(np.repeat(np.nan, i)).append(data[column][:-i] , ignore_index=True)
        return self._remove_previous_null_data(data)

    def _convert_target_to_int(self, data):
        data['target_temp'] = (data['target_temp'] * 100).astype(int)
        return data

    def _transform_with_pipelines(self, original_data):
        category_columns = []
        for i in range(1, 12):
            category_columns.append('month_{}'.format(i))
        for i in self.hour_diffs:
            for column in self.weather_main_columns:
                category_columns.append('{}_{}_ago'.format(column, i))
        category_columns += self.weather_main_columns
        numercial_columns = list(set(original_data) - set(category_columns))

        cat_pipeline = Pipeline([
            ('selector', DataFrameSelector(category_columns))
        ])

        num_pipeline = Pipeline([
            ('selector', DataFrameSelector(numercial_columns)),
            ('scalar', StandardScaler())
        ])

        full_pipeline = FeatureUnion(transformer_list=[
            ("num_pipeline", num_pipeline),
            ("cat_pipeline", cat_pipeline)

        ])

        return full_pipeline.fit_transform(original_data)



    def _remove_previous_null_data(self, data):
        return data[48:-1], data[-1]

    def _add_new_data(self, data):
        data['dt_datetime'] =  pd.to_datetime(data['dt_iso'], format='%Y-%m-%d %H:%M:%S +%f %Z')
        data = self._transform_categorical_data(data)
        data['target_temp'] = data['temp'][1:].append(pd.Series([np.nan]) , ignore_index=True)
        return data

    def _transform_datetime(self, current_data):
        for month in range(1, 12):
            current_data['month_{}'.format(month)] = 1 if current_data['dt_datetime'].month == month else 0

        current_data['year'] =  current_data['dt_datetime'].year
        current_data['dayofweek'] = current_data['dt_datetime'].dayofweek
        current_data['dayofyear'] = current_data['dt_datetime'].dayofyear
        current_data['hourofday'] = current_data['dt_datetime'].hour
        return current_data

    def _transform_categorical_data(self, data):
        for column in self.weather_description_columns:
            data[column] = data['weather_description'] == column
            data[column] = data[column].astype(int)

        for column in self.weather_main_columns:
            data[column] = data['weather_main'] == column
            data[column] = data[column].astype(int)
        return data

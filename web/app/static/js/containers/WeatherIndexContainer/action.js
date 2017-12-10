import {
  FETCH_WEATHER,
  WEATHER_PATH,
  REPROCESS,
} from './constants';

import axios from 'axios';

export function fetchWeather() {
  const request = axios.get(WEATHER_PATH);

  return (dispatch) => {

    return request
      .then((response) => {
        dispatch(fetchWeatherSuccess(response.data));
      })
      .catch((error) => {
        console.error(error);
        dispatch(fetchWeatherFailure(error.data));
      });
  };
}

function fetchWeatherSuccess(weather) {
  return {
    type: FETCH_WEATHER.SUCCESS,
    payload: { weather },
  };
}

function fetchWeatherFailure(errorMessage) {
  return {
    type: FETCH_WEATHER.FAILURE,
    payload: { errorMessage },
  };
}

export function reprocess() {
  const request = axios.post(WEATHER_PATH);
  return (dispatch) => {
    dispatch(reprocessRequest());
    return request
      .then(() => dispatch(reprocessSuccess()))
      .catch((error) => {
        console.error(error);

        dispatch(reprocessFailure(error.data));
      });
  };
}


function reprocessRequest() {
  return {
    type: REPROCESS.REQUEST,
  };
}

function reprocessSuccess() {
  return {
    type: REPROCESS.SUCCESS
  };
}

function reprocessFailure(errorMessage) {
  return {
    type: REPROCESS.FAILURE,
    payload: { errorMessage },
  };
}

function createRequestTypes(base) {
  const requestType = {};
  ['REQUEST', 'SUCCESS', 'FAILURE'].forEach((type) => {
    requestType[type] = `${base}_${type}`;
  });
  return requestType;
}

export const FETCH_WEATHER = createRequestTypes('weather');
export const WEATHER_PATH = '/api/v1/predictions';

export const REPROCESS = createRequestTypes('reprocess');
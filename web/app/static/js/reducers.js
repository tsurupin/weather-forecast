import { combineReducers } from 'redux';

import weatherIndex from './containers/WeatherIndexContainer/reducer';

const routeReducer = combineReducers({
  weatherIndex
});

export default routeReducer;
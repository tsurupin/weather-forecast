import React from 'react';
import { BrowserRouter as Router, Route } from 'react-router-dom'
import { Provider } from 'react-redux';
import {
  createStore,
  applyMiddleware,
} from 'redux';
import thunk from 'redux-thunk';
import reducers from './reducers';

const store = createStore(reducers, applyMiddleware(thunk));
import { WeatherIndexPage } from './pages';
import { App } from './containers';

export default(
  <Provider store={store}>
    <Router>
      <Route exact path="/" component={WeatherIndexPage} />
    </Router>
  </Provider>
);
import React from 'react';
import { Router, Route, IndexRoute, browserHistory } from 'react-router';
import { Provider } from 'react-redux';
import {
  createStore,
  applyMiddleware,
} from 'redux';
import thunk from 'redux-thunk';
import reducers from './reducers';

const store = createStore(reducers, applyMiddleware(thunk));
import { WeatherIndex } from './pages';
import { App } from './containers';

export default(
  <Provider store={store}>
    <Router
      onUpdate={() => {
        document.getElementById('app').focus();
        window.scrollTo(0, 0);
      }}
      history={browserHistory}
    >
      <Route path="/" component={App} >
        <IndexRoute component={WeatherIndex} />
      </Route>
    </Router>
  </Provider>
);
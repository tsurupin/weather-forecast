import React, { Component } from 'react';

import {
  WeatherIndexContainer,
} from '../../containers';

class WeatherIndexPage extends Component {

  constructor(props) {
    super(props);
  }
  render() {
    return (
      <div>
        <WeatherIndexContainer />
      </div>
    );
  }

}

export default WeatherIndexPage;
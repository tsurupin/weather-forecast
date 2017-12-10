import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { bindActionCreators } from 'redux';

import * as WeatherIndexActionCreators from './action';
import { PredictionTable, HistoryTable, ReprocessButton } from '../../components';
import {
  Wrapper
} from './styles';

const propTypes = {
  actions: PropTypes.shape({
    fetchWeather: PropTypes.func.isRequired,
    reprocess: PropTypes.func.isRequired
  }).isRequired
};

function mapStateToProps({ weatherIndex }) {
  const { predictions, pastRecords, submitting } = weatherIndex;
  return {
    predictions,
    pastRecords,
    submitting,
  };
}
function mapDispatchToProps(dispatch) {
  return {
    actions: bindActionCreators(
      WeatherIndexActionCreators,
      dispatch,
    ),
  };
}
class WeatherIndexContainer extends Component {
  constructor(props) {
    super(props);
    this.handleLoad = this.handleLoad.bind(this);
    this.handleReprocess = this.handleReprocess.bind(this);
  }

  handleLoad() {
    this.props.actions.fetchWeather();
  }

  handleReprocess() {
    this.props.actions.reprocess();
  }

  render() {
    const { predictions, historyRecords, submitting } = this.props;

    return (
      <Wrapper>
        <PredictionTable predictions={predictions}/>
        <HistoryTable historyRecords={historyRecords} />
        <ReprocessButton submitting={submitting} />
      </Wrapper>
    );
  }
}

WeatherIndexContainer.peropTypes = propTypes;
export default connect(mapStateToProps, mapDispatchToProps)(WeatherIndexContainer);
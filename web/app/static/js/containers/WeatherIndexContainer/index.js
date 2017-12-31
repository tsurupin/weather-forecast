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

  componentWillMount() {
    this.handleLoad();
  }


  handleLoad() {
    console.log("load data--------------");
    this.props.actions.fetchWeather();
  }

  handleReprocess() {
    console.info("handle-process!!")
    this.props.actions.reprocess();
  }

  render() {
    const { predictions, pastRecords, submitting } = this.props;

    console.log(predictions);
    return (
      <Wrapper>
        <PredictionTable predictions={predictions}/>
        {/*<HistoryTable historyRecords={pastRecords} />*/}
        <ReprocessButton submitting={submitting} handleReprocess={this.handleReprocess} />
      </Wrapper>
    );
  }
}

WeatherIndexContainer.propTypes = propTypes;
export default connect(mapStateToProps, mapDispatchToProps)(WeatherIndexContainer);

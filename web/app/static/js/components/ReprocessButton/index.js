import React from 'react';
import PropTypes from 'prop-types';
import { Wrapper } from './styles';

const propTypes = {
  submitted: PropTypes.bool.isRequired
};

const defaultProps = {
  submitted: false
};

const ReprocessButton = ({submitted, handleReprocess}) => {
  <Wrapper
    type="button"
    onClick={ () => handleReprocess()}
    disabled={submitted}
  >
    Reprocess!
  </Wrapper>
}

ReprocessButton.ropTypes = propTypes;
ReprocessButton.defaultProps = defaultProps;
export default ReprocessButton;
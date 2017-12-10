import React from 'react';
import PropTypes from 'prop-types';
import { Wrapper } from './styles';

const propTypes = {
  submitted: PropTypes.bool.isRequired
};

const defaultProps = {
  submitted: false
};

const ReprocessButton = ({submitting, handleReprocess}) => {
  <Wrapper
    type="button"
    onClick={ () => handleReprocess()}
    disabled={submitting}
  >
    Reprocess!
  </Wrapper>
}

ReprocessButton.ropTypes = propTypes;
ReprocessButton.defaultProps = defaultProps;
export default ReprocessButton;
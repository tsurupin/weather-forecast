import React from 'react';
import PropTypes from 'prop-types';
import { Wrapper, Thead, Tbody, Tr, Th, Td } from './styles';

const propTypes = {

};

const defaultProps = {
  predictions: []
};

const PredictionTable = ({predictions}) => {
  <Wrapper>
      <Thead>
        <Tr>
          {renderList('day', predictions)}
        </Tr>
      </Thead>
    <Tbody>
      ['condition', 'precipitation_percent'].map(key => (
        return(
          <Tr>
            {renderList(key, predictions)}
          </Tr>
        )
      )
    </Tbody>
  </Wrapper>
}


function renderList(key, predictions) {
  let lists = [];
  lists.append(<Td>{key}</Td>);
  predictions.forEach(prediction => (
    lists.append(<Td>{prediction[key]}</Td>)
  ))
  return lists;
}

PredictionTable.propTypes = propTypes;
PredictionTable.defaultProps = defaultProps;
export default PredictionTable;
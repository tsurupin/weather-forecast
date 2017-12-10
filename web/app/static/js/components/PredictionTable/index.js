import React from 'react';
import PropTypes from 'prop-types';
import { Wrapper, Thead, Tbody, Tr, Th, Td } from './styles';

const propTypes = {

};

const defaultProps = {
  predictions: []
};

const PredictionTable = ({predictions}) => {
  return (
      <Wrapper>
        <Thead>
          {renderListComponent('day', predictions)}
        </Thead>
        <Tbody>
          {renderList(predictions)}
        </Tbody>
      </Wrapper>
  )
}


const listNames = ['condition', 'precipitation_percent'];

function renderList(predictions) {
  return listNames.map((keyName) => {
    renderListComponent(keyName, predictions);
  });
}


function renderListComponent(keyName, predictions) {
  let lists = [];
  lists = [lists, (<Tr key={keyName}><Td key={keyName}>{keyName}</Td></Tr>)]
  predictions.forEach((prediction, i) => (
    lists = [lists, (<Tr key={i}><Td key={i}>{prediction[keyName]}</Td></Tr>)]
  ));
  return lists;
}


PredictionTable.propTypes = propTypes;
PredictionTable.defaultProps = defaultProps;
export default PredictionTable;
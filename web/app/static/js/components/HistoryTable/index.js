import React from 'react';
import PropTypes from 'prop-types';
import { Wrapper, Thead, Tbody, Tr, Th, Td } from './styles';

const propTypes = {

};

const defaultProps = {
  pastRecords: []
};

const HistoryTable = ({pastRecords}) => {
  return (
    <Wrapper>
      <Thead>
        {renderListComponent('measured_at', pastRecords)}
      </Thead>
      <Tbody>
        {renderList(pastRecords)}
      </Tbody>
    </Wrapper>
  )
}

const listNames = ['condition', 'condition_details', 'temperature'];

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


HistoryTable.propTypes = propTypes;
HistoryTable.defaultProps = defaultProps;
export default HistoryTable;
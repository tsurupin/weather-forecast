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

function renderList(pastRecords) {
  return listNames.map((keyName) => {
    renderListComponent(keyName, pastRecords);
  });
}


function renderListComponent(keyName, pastRecords) {

  let lists = [];
  lists = [...lists, (<Tr key={keyName}><Td key={keyName}>{keyName}</Td></Tr>)]
  pastRecords.forEach((pastRecord, i) => (
    lists = [...lists, (<Tr key={i}><Td key={i}>{pastRecord[keyName]}</Td></Tr>)]
  ));

  return lists;
}


HistoryTable.propTypes = propTypes;
HistoryTable.defaultProps = defaultProps;
export default HistoryTable;
import React from 'react';
import PropTypes from 'prop-types';
import { Wrapper, Thead, Tbody, Tr, Th, Td } from './styles';

const propTypes = {

};

const defaultProps = {
  pastRecords: []
};

const HistoryTable = ({pastRecords}) => {
  <Wrapper>
      <Thead>
        <Tr>
          {renderList('measured_at', pastRecords)}
        </Tr>
      </Thead>
    <Tbody>
      ['condition', 'condition_details', 'temperature'].map(keyName => (
        return(
          <Tr>
            {renderList(keyName, pastRecords)}
          </Tr>
        )
      )
    </Tbody>
  </Wrapper>
}


function renderList(key, pastRecords) {
  let lists = [];
  lists.append(<Td>{key}</Td>);
  pastRecords.forEach(prediction => (
    lists.append(<Td>{prediction[key]}</Td>)
  ))
  return lists;
}

HistoryTable.propTypes = propTypes;
HistoryTable.defaultProps = defaultProps;
export default HistoryTable;
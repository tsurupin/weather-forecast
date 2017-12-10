import {
  FETCH_WEATHER,
  REPROCESS,
} from './constants';

const INITIAL_STATE = {
  predictions: [],
  pastRecords: [],
  submitting: false,
  errorMessage: undefined,
};

export default function (state = INITIAL_STATE, action) {
  switch (action.type) {

    case REPROCESS.REQUEST:
      return { ...state, submitting: true };
    case FETCH_WEATHER.SUCCESS:
      const { predictions, pastRecords } = action.payload;
      return { ...state, predictions, pastRecords };

    case REPROCESS.SUCCESS:
      return { ...state, submitting: false };

    case FETCH_WEATHER.FAILURE:
    case REPROCESS.FAILURE:
      const { errorMessage } = action.payload;
      return { ...state, errorMessage, submitting: false };

    default:
      return state;
  }
}
import { embeddedDlq } from './embedded.js';

type Operations = ReturnType<typeof embeddedDlq>;

export const getDlqConfig: Operations['getDlqConfig'] = (...args) =>
  embeddedDlq().getDlqConfig(...args);
export const getDlqEntries: Operations['getDlqEntries'] = (...args) =>
  embeddedDlq().getDlqEntries(...args);
export const getDlqStats: Operations['getDlqStats'] = (...args) =>
  embeddedDlq().getDlqStats(...args);
export const retryDlqByFilter: Operations['retryDlqByFilter'] = (...args) =>
  embeddedDlq().retryDlqByFilter(...args);

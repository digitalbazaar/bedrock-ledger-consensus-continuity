
const brLedgerNode = require('bedrock-ledger-node');

const api = {};
module.exports = api;

api.hasher = brLedgerNode.consensus._hasher;

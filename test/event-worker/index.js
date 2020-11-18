const bedrock = require('bedrock');
require('bedrock-server');
require('bedrock-ledger-consensus-continuity');

require('./config');

bedrock.start();

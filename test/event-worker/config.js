const bedrock = require('bedrock');
const {config} = bedrock;

// MongoDB
config.mongodb.name = 'bedrock_ledger_continuity_test';

config.server.port = 19443;
config.server.httpPort = 19080;

const cfg = config['ledger-consensus-continuity'];
cfg.gossip.batchProcess.enable = true;
cfg.consensus.workerpool.enabled = false;

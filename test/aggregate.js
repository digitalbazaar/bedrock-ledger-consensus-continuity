/* eslint-disable */

/* globals db */
db = db.getSiblingDB('bedrock_ledger_continuity_test');
// x = db.getCollectionNames();
// printjson(x);
// cursor = db['aa0d6eda-147d-469a-a4ef-075872701060-event'].find();
// while (cursor.hasNext()) {
//   printjson(cursor.next());
// }

// NOTE: THIS WORKS
cursor = db['aa0d6eda-147d-469a-a4ef-075872701060-event'].aggregate([{
  $graphLookup: {
    from: 'aa0d6eda-147d-469a-a4ef-075872701060-event',
    startWith: 'ni:///sha-256;bYl1UH1BmuDBl-vTcxHjbborDXP--lV_IzyqDUOlKrM',
    connectFromField: "event.treeHash",
    connectToField: "eventHash",
    as: "_parents",
    restrictSearchWithMatch: {
      'event.type': 'ContinuityMergeEvent',
      eventHash: {$ne: 'ni:///sha-256;bYl1UH1BmuDBl-vTcxHjbborDXP--lV_IzyqDUOlKrM'},
      'meta.continuity2017.creator': {$ne: 'https://bedrock.local:18443/consensus/continuity2017/peers/43886ca4-3b9c-40b1-82b1-89450cde8916'}
    }
  }
}])
while (cursor.hasNext()) {
  printjson(cursor.next());
}

// NOTE: THIS DOES NOT WORK
cursor = db['aa0d6eda-147d-469a-a4ef-075872701060-event'].aggregate([{
  $graphLookup: {
    from: 'aa0d6eda-147d-469a-a4ef-075872701060-event',
    startWith: 'ni:///sha-256;bYl1UH1BmuDBl-vTcxHjbborDXP--lV_IzyqDUOlKrM',
    connectFromField: ["event.treeHash", "event.parentHash"],
    connectToField: "eventHash",
    as: "_parents",
    restrictSearchWithMatch: {
      'event.type': 'ContinuityMergeEvent',
      eventHash: {$ne: 'ni:///sha-256;bYl1UH1BmuDBl-vTcxHjbborDXP--lV_IzyqDUOlKrM'},
      'meta.continuity2017.creator': {$ne: 'https://bedrock.local:18443/consensus/continuity2017/peers/43886ca4-3b9c-40b1-82b1-89450cde8916'}
    }
  }
}])
while (cursor.hasNext()) {
  printjson(cursor.next());
}

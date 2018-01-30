'use strict'

var mongodb = require('mongodb');

let connection_uri;
let cachedDb = null;

console.log('Loading function');

// export handeler
exports.handler = (event, context, callback) => {
  var uri = process.env['MONGODB_URI'];
  
  if (connection_uri != null) {
    processEvent(event, context, callback);
  } else {
    connection_uri = uri;
    console.log('the connection string is ' + connection_uri);
    processEvent(event, context, callback);
  }
};

// process event
function processEvent(event, context, callback) {
  var jsonContents = JSON.parse(JSON.stringify(event));

  console.log('Calling MongoDB from AWS Lambda with event: ' + JSON.stringify(event));

  //the following line is critical for performance reasons to allow re-use of database connections across calls to this Lambda function and avoid closing the database connection. The first call to this lambda function takes about 5 seconds to complete, while subsequent, close calls will only take a few hundred milliseconds.
  context.callbackWaitsForEmptyEventLoop = false;

  try {
    // if cacheDB
    if (cachedDb == null) {
      console.log('=> connecting to database');
      // connect
      mongodb.MongoClient.connect(connection_uri, function(err, db) {
        cachedDb = db;
        return createDoc(db, jsonContents, callback);
      });
    } else {
      return createDoc(db, jsonContents, callback);
    }
  } catch (err) {
    console.error("an error occurred in createDoc", err);
    callback(null, JSON.stringify(err));
  }
}

// create doc
function createDoc(db, json, callback) {
  db.collection('events').insertOne(json, function(err, result) {
    // if there is an error inserting
    if (err != null) {
      console.error("an error occurred in createDoc", err);
      callback(null, JSON.stringify(err));
    } else {
      console.log("Event created: " + result.insertedId);
      callback(null, "SUCCESS");
    }
    //we don't need to close the connection thanks to context.callbackWaitsForEmptyEventLoop = false (above)
    //this will let our function re-use the connection on the next called (if it can re-use the same Lambda container)
    //db.close();
  });
}

"use strict";

var request = require("request");
var fs      = require("fs");
var util    = require("util");
var stream  = require("stream");
var parse   = require('csv-parse');
var es      = require("event-stream");
var _       = require("lodash");
var uuid    = require("uuid");
var join    = require("path").join;
var AWS     = require("aws-sdk");

var dynamo  = new AWS.DynamoDB({region:"us-west-2"});
var csvFile   = process.argv[2];
var tableName = process.argv[3];
var filestream = fs.createReadStream(csvFile);
var url     = "https://casepackevents-uat.mas.runway.nonprod.aws.cloud.nordstrom.net";

console.log("Importing csv into DynamoDB. Please wait.");

var _getAbsolutePath = function(path) {
  var res = null,
    homeDir = process.env.HOME || process.env.USERPROFILE;

  var windowsRegex = /([A-Z|a-z]:\\[^*|"<>?\n]*)|(\\\\.*?\\.*)/;

  if (path.match(/^\//) || path.match(windowsRegex)) {
    //On Windows and linux
    res = path;
  } else {
    if (path === '~') {
      //On linux only
      res = homeDir;
    } else if (path.slice(0, 2) !== '~/') {
      //On Windows and linux
      res = join(process.cwd(), path);
    } else {
      //On linux only
      res = join(homeDir, path.slice(2));
    }
  }
  return res;
};

var _loadAWSCredentials = function(path) {
  //default parameter
  var profileName = arguments.length <= 1 ||
  arguments[1] === undefined ||
  arguments[1] === null ? 'default' : arguments[1];

  var fs = require('fs'),
    dataRaw = fs.readFileSync(_getAbsolutePath(path)),
    data = dataRaw.toString();

  var regex = new RegExp('\\[' + profileName +
      '\\](.|\\n|\\r\\n)*?aws_secret_access_key( ?)+=( ?)+(.*)'),
    match;
  if ((match = regex.exec(data)) !== null) {
    process.env['AWS_SECRET_ACCESS_KEY'] = match[4];
  } else {
    console.log('WARNING: Couldn\'t find the \'aws_secret_access_key\' field inside the file.');
  }

  regex = new RegExp('\\[' + profileName + '\\](.|\\n|\\r\\n)*?aws_access_key_id( ?)+=( ?)+(.*)');
  if ((match = regex.exec(data)) !== null) {
    process.env['AWS_ACCESS_KEY_ID'] = match[4];
  } else {
    console.log('WARNING: Couldn\'t find the \'aws_access_key_id\' field inside the file.');
  }

  regex = new RegExp('\\[' + profileName + '\\](.|\\n|\\r\\n)*?aws_session_token( ?)+=( ?)+(.*)');
  if ((match = regex.exec(data)) !== null) {
    process.env['AWS_SESSION_TOKEN'] = match[4];
  } else {
    console.log('WARNING: Couldn\'t find the \'aws_session_token\' field inside the file.');
  }
};

_loadAWSCredentials("~/.aws/credentials");

// var ddb     = require('dynamodb').ddb({
//   accessKeyId: process.env['AWS_ACCESS_KEY_ID'],
//   secretAccessKey: process.env['AWS_SECRET_ACCESS_KEY'],
//   port: 443,
//   // sessionToken: process.env['AWS_SESSION_TOKEN'],
//   // sessionExpires: 60,
//   endpoint: 'dynamodb.us-west-2.amazonaws.com',
// });


filestream.pipe(es.split())
    .pipe(es.mapSync(function (line) {
        filestream.pause();

        var row = line.split(",");
        var item = {
          EventId: row[0],
          CasePackId: row[1],
          CorrelationId: row[2],
          EventDateTime: row[3],
          EventType: row[4],
          Market: row[5],
          SellingChannel: row[6],
          Status: row[7]
        };

        var params = {
          TableName: tableName,
          Item: {
            EventId: { S:row[0] },
            CasePackId: { S:row[1] },
            CorrelationId: { S:row[2] },
            EventDateTime: { S:row[3] },
            EventType: { S:row[4] },
            Market: { S:row[5] },
            SellingChannel: { S:row[6] },
            Status: { S:row[7] }
          },
          ReturnConsumedCapacity: "TOTAL"
        };

        dynamo.putItem(params, function(err, data) {
          if (err) {
            console.log(err);
          }

        });

        // ddb.putItem(tableName, item, {}, function(err, res, cap) {
        //   if (err) {
        //     console.log(err);
        //   }
        //   if (res) {
        //     console.log(res);
        //   }
        //   if (cap) {
        //     console.log(cap);
        //   }
        // });

        filestream.resume();
    })
        .on("error", function () {
            console.log("Error while reading file.");
        })
        .on("end", function () {
            console.log("Read entire file.")
        })
    );

/**
 * Remove empty attributes from obj so that dynamodb does not complain
 */
function clean(obj) {
    var propNames = Object.getOwnPropertyNames(obj);
    for (var i = 0; i < propNames.length; i++) {
        var propName = propNames[i];
        if (typeof obj[propName] === "object") {
            this.clean(obj[propName]);  //Recursively clean the subObject
        } else if (obj[propName] === null || obj[propName] === undefined || obj[propName] === "") {
            delete obj[propName];
        }
    }
    return obj;
}
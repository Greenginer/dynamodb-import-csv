'use strict';

var AWS = require("aws-sdk");
var fs = require('fs')
    , util = require('util')
    , stream = require('stream')
    , es = require('event-stream');
var _ = require('lodash');
var uuid = require("uuid");
//var readline = require('readline'),

var docClient = new AWS.DynamoDB.DocumentClient();

console.log("Importing csv into DynamoDB. Please wait.");


var filestream = fs.createReadStream('all_india_pin_code.csv');

filestream.pipe(es.split())
    .pipe(es.mapSync(function (line) {
        filestream.pause();
        //console.log(line);
        var columns = ['officename', 'pincode', 'officeType', 'deliverystatus', 'divisionname', 'regionname', 'circlename', 'taluk', 'districtname', 'statename'];
        var splitline = line.split(',').map((item, index) => {
            var columnName = columns[index]

            return {
                [columns[index]]: item.trim()
            }
        })

        var item = {};

        for (var i = 0; i < splitline.length; i++) {
            _.merge(item, splitline[i]);
        }

        item.id = uuid.v4()

        var params = {
            TableName: "pincode",
            Item: clean(item)
        };

        docClient.put(params, (error, data) => {
            if (error) {
                console.log("Error occured: ", error)
            } else {
                //console.log("Stored successfully: ", item)
            }
        })
        filestream.resume();
    })
        .on('error', function () {
            console.log('Error while reading file.');
        })
        .on('end', function () {
            console.log('Read entire file.')
        })
    );

/**
 * Remove empty attributes from obj so that dynamodb soes not complain
 */
function clean(obj) {
    var propNames = Object.getOwnPropertyNames(obj);
    for (var i = 0; i < propNames.length; i++) {
        var propName = propNames[i];
        if (typeof obj[propName] === 'object') {
            this.clean(obj[propName]);  //Recursively clean the subObject
        } else if (obj[propName] === null || obj[propName] === undefined || obj[propName] === '') {
            delete obj[propName];
        }
    }
    return obj;
}
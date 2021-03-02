const AWS = require('aws-sdk');
const csv = require('csvtojson');
const csvFilePath = './ex.csv';

// @TODO:
// dynamic field = [keyslist, phasorList]
// dynamic field is static, defined in Kinesis Message Model
// in DynamoSchema, take dynamic, parse it, use keys for database keys and values as the values

// for sRATE, compute before kinesis stream and send along
// for gpsLock and error, can either do that before or after
AWS.config.update(
    {
        region: "us-east-1"
    }
);

var fs = require('fs');
let kinesis = new AWS.Kinesis()

var fileContents = fs.readFileSync(csvFilePath);
var lines = fileContents.toString().split('\n');

for (var i = 1; i < 3; i++) {
    row = lines[i].split(',');
    // console.log(getTimeStamp(row[2] + ' ' + row[3]));
    if (i == 1) {
        // console.log(row[2]);
        time1 = getTimeStamp(row[2] + ' ' + row[3]) 
    } else if (i == 2) {
        // console.log(row[2]);
        time2 = getTimeStamp(row[2] + ' ' + row[3])
    }
}
israte = 1 / (time2 - time1) * 1000
srate = 2 * Math.round(israte / 2)

let loop1 = () => {
    i = 0
    var header = lines[0];
    for (var i = 1; i < 3; i++) {
        row = lines[i].split(',');
        parameters = {
            Data: JSON.stringify({
                // ID: 99999,
                ID: row[0],
                Date: arangeTime(row[2]),
                Time: row[3],
                Status: row[4],
                Frequency: row[5],
                sRate: srate,
                // Dynamic: [header[7], row[7], row[8]]
            }), 
            PartitionKey: "key" + toString(i),
            StreamName: "teststream"
        }
        kinesis.putRecord(parameters, function(err, data) {
            if (err) {
                console.log('\n WARNING: Could not add record \n');
                console.log(err, err.stack); // an error occurred
            }
            else {  
                console.log('\n Record with following data added to stream: \n');  
                console.log(data);           // successful response
            }
        });
    }
}

function getTimeStamp(input) {
    var parts = input.trim().split(' ');
    var date = parts[0].split('/');
    var time = parts[1].split(':');
    var ms = parts[1].split('.');
    // NOTE:: Month: 0 = January - 11 = December.
    var d = new Date(date[2],date[0]-1,date[1],time[0],time[1],time[2],ms[1]);
    return d.getTime();
}

function arangeTime(input) {
    // 1/7/2021 to 2021-01-07
    var parts = input.trim().split('/');
    var month = null;
    var day = null;
    if (parts[0].length != 2) {
        month = '0'.concat(parts[0]);
    }
    if (parts[1].length != 2) {
        day = '0'.concat(parts[1]);
    }
    return parts[2].concat("-",month,"-",day)
}



loop1();
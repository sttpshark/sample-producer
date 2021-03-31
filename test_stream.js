const AWS = require('aws-sdk');
const lineReader = require('line-reader');
var readline = require('readline');
const fs = require('fs');

AWS.config.update(
    {
        region: "us-east-1"
    }
);

let kinesis = new AWS.Kinesis()

let arr = new Array();
lineReader.eachLine(process.stdin, function(line) {
    arr.push(line);
    if (line.includes(' ')) {
        if (arr.length > 8) {
        console.log(arr);
        let parameters = {
            Data: {
                "pmuID": arr[1],
                "ts": arr[2],
                "status": arr[3],
                "gpsLock": hex2bin(arr[3].replace(/\s/g, ""))[13] ^ 1,      // Status returns 0 if gpsLock is on, but for our schema 1 means gpsLock is on, hence the XOR
                "sRate": arr[4],
                "error": hex2bin(arr[3].replace(/\s/g, ""))[14],
                "frequency": arr[5],
                "dfdt": arr[6]
            },
            PartitionKey: "key" + 1,
            StreamName: "teststream",
        }
        // first 7 columns of data are not phasors
        // create Phasors object, loop through CSV and attach Phasor Names to magnitude and angle
        // add data to Phasors object
        let Phasors = {};
        // console.log(arr.length)
        for (var j = 7; j < arr.length - 1; j += 3) {
            Object.defineProperty(Phasors, arr[j], {
                value: {"mag": arr[j+2], "angle": arr[j+1]},
                writable: true,
                enumerable: true,
                configurable: true
            });
        }
        // convert phasors object to string (Kinesis requirement)
        Phasors = JSON.stringify(Phasors)

        // add phasors object / string to Kinesis data payload
        Object.defineProperty(parameters.Data, "phasors", {
            value: Phasors,
            writeable: true,
            enumerable: true,
            configurable: true
            });

        // convert data payload to string (Kinesis requirement)
        parameters.Data = JSON.stringify(parameters.Data)
        kinesis.putRecord(parameters, function(err, data) {
            if (err) {
                console.log('\n WARNING: Could not add record \n');
                console.log(err, err.stack); // an error occurred
            }
            else {  
                console.log('\n Record with following data added to stream: \n');  
                console.log(data);           // successful response
            }
        })
    }
    arr = [];
    // return false
    }
});

// helper functions:

function hex2bin(hex){
    let binStr = hex.toString(2);

    while(binStr.length < 16) {
        binStr = "0" + binStr;
    }
    return binStr
}

// const stdin = process.stdin;
// let data = new Array();

// stdin.setEncoding('utf8');
// stdin.on('data', function (chunk) {
//     chunk = chunk.replace(/(\r\n|\n|\r)/gm, " ");
//     let xyz = chunk.split(' ');
//     data.push(xyz);

// });

// stdin.on('end', function () {
//     console.log(data)
//     let arr = data[0]
//         let parameters = {
//             Data: {
//                 "pmuID": arr[0],
//                 "ts": arr[1],
//                 "status": arr[2],
//                 "gpsLock": hex2bin(arr[2].replace(/\s/g, ""))[13] ^ 1,      // Status returns 0 if gpsLock is on, but for our schema 1 means gpsLock is on, hence the XOR
//                 "sRate": arr[3],
//                 "error": hex2bin(arr[2].replace(/\s/g, ""))[14],
//                 "frequency": arr[4],
//                 "dfdt": arr[5]
//             },
//             PartitionKey: "key" + 1,
//             StreamName: "teststream",
//         }
//         // first 7 columns of data are not phasors
//         // create Phasors object, loop through CSV and attach Phasor Names to magnitude and angle
//         // add data to Phasors object
//         let Phasors = {};
//         // console.log(arr.length)
//         for (var j = 6; j < arr.length - 1; j += 3) {
//             Object.defineProperty(Phasors, arr[j], {
//                 value: {"mag": arr[j+2], "angle": arr[j+1]},
//                 writable: true,
//                 enumerable: true,
//                 configurable: true
//             });
//         }
//         // convert phasors object to string (Kinesis requirement)
//         Phasors = JSON.stringify(Phasors)

//         // add phasors object / string to Kinesis data payload
//         Object.defineProperty(parameters.Data, "phasors", {
//             value: Phasors,
//             writeable: true,
//             enumerable: true,
//             configurable: true
//             });

//         // convert data payload to string (Kinesis requirement)
//         parameters.Data = JSON.stringify(parameters.Data)
//         kinesis.putRecord(parameters, function(err, data) {
//             if (err) {
//                 console.log('\n WARNING: Could not add record \n');
//                 console.log(err, err.stack); // an error occurred
//             }
//             else {  
//                 console.log('\n Record with following data added to stream: \n');  
//                 console.log(data);           // successful response
//             }
//         });
// });

// stdin.on('error', console.error);


// // helper functions:

// function hex2bin(hex){
//     let binStr = hex.toString(2);

//     while(binStr.length < 16) {
//         binStr = "0" + binStr;
//     }
//     return binStr
// }



// const AWS = require('aws-sdk');
// const csvFilePath = './ex.csv';

// AWS.config.update(
//     {
//         region: "us-east-1"
//     }
// );

// var fs = require('fs');
// let kinesis = new AWS.Kinesis()

// // read csv file using fs and go through file line by line
// var fileContents = fs.readFileSync(csvFilePath);
// var lines = fileContents.toString().split('\n');

// // calculate the sample rate by taking difference b/t timestamp of two consecutive rows 
// // and take recipricol
// for (var z = 1; z < 3; z++) {
//     let row = lines[z].split(',');
//     // console.log(getTimeStamp(row[2] + ' ' + row[3]));
//     if (z == 1) {
//         time1 = getTimeStamp(row[2] + ' ' + row[3]) 
//     } else if (z == 2) {
//         time2 = getTimeStamp(row[2] + ' ' + row[3])
//     }
// }
// let israte = 1 / (time2 - time1) * 1000
// let srate = 2 * Math.round(israte / 2)

// let loop2 = () => {
//     let keys = lines[0].split(",");
//     for (var i = 1; i < 3; i++) {
//         console.log(i);
//         let row = lines[i].split(',');
//         let parameters = {
//             Data: {
//                 "pmuID": row[0],
//                 "ts": getTimeStamp(row[2] + ' ' + row[3]),
//                 "status": '00 00',
//                 "gpsLock": hex2bin(row[4].replace(/\s/g, ""))[13] ^ 1,      // Status returns 0 if gpsLock is on, but for our schema 1 means gpsLock is on, hence the XOR
//                 "sRate": srate,
//                 "error": hex2bin(row[4].replace(/\s/g, ""))[14],
//                 "frequency": row[5],
//                 "dfdt": row[6]
//             },
//             PartitionKey: "key" + toString(i),
//             StreamName: "teststream",
//         }
//         // first 7 columns of data are not phasors
//         // create Phasors object, loop through CSV and attach Phasor Names to magnitude and angle
//         // add data to Phasors object
//         let Phasors = {};
//         for (var j = 0; j < (row.length - 7); j += 2) {
//             Object.defineProperty(Phasors, [keys[j + 7].split(' ')[0]], {
//                 value: {"mag": Number(row[j + 7]), "angle": Number(row[j+8])},
//                 writable: true,
//                 enumerable: true,
//                 configurable: true
//             });
//         }
//         // convert phasors object to string (Kinesis requirement)
//         Phasors = JSON.stringify(Phasors)

//         // add phasors object / string to Kinesis data payload
//         Object.defineProperty(parameters.Data, "phasors", {
//             value: Phasors,
//             writeable: true,
//             enumerable: true,
//             configurable: true
//             });

//         // convert data payload to string (Kinesis requirement)
//         parameters.Data = JSON.stringify(parameters.Data)
//         kinesis.putRecord(parameters, function(err, data) {
//             if (err) {
//                 console.log('\n WARNING: Could not add record \n');
//                 console.log(err, err.stack); // an error occurred
//             }
//             else {  
//                 console.log('\n Record with following data added to stream: \n');  
//                 console.log(data);           // successful response
//             }
//         });
//     }
// }

// // below are helper functions for processing data: 

// // converts hex of status to 16 bit binary string for proper processing
// function hex2bin(hex){
//     let binStr = hex.toString(2);

//     while(binStr.length < 16) {
//         binStr = "0" + binStr;
//     }
//     return binStr
// }
// // take date and time and return timestamp in unix time (ms)
// function getTimeStamp(input) {
//     var parts = input.trim().split(' ');
//     var date = parts[0].split('/');
//     var time = parts[1].split(':');
//     var ms = parts[1].split('.');
//     // NOTE:: Month: 0 = January - 11 = December.
//     var d = new Date(date[2],date[0]-1,date[1],time[0],time[1],time[2],ms[1]);
//     return d.getTime();
// }

// // run the script

// loop2();







// The digits in the status column are HEX numbers. Each “0” is corresponding to four binary “0”. 
// In other words, the number of attributes are not only 5. There are 16 attributes according to the protocol standard IEEE C37.118-2005

// The meaning of the binary code is defined below:  C37.118-2005

// STAT              2 bytes             Bitmapped flags (0-15, totally 16 bits).

// Bit 15: Data valid, 0 when PMU data is valid, 1 when invalid or PMU is in test mode.
// Bit 14: PMU error including configuration error, 0 when no error.
// Bit 13: PMU sync, 0 when in sync (GPS locked)
// Bit 12: Data sorting, 0 by time stamp, 1 by arrival.
// Bit 11: PMU trigger detected, 0 when no trigger.
// Bit 10: Configuration changed, set to 1 for 1 min when configuration changed.
// Bits 09–06: Reserved for security, presently set to 0.
// Bits 05–04: Unlocked time: 00 = sync locked, best quality
        // **01 = Unlocked for 10 s
        // **10 = Unlocked for 100 s
        // **11 = Unlocked over 1000 s
// Bits 03–00: Trigger reason:
        // **1111–1000: Available for user definition
        // **0111: Digital 0110: Reserved
        // **0101: df/dt high 0100: Frequency high/low
        // **0011: Phase-angle diff 0010: Magnitude high
        // **0001: Magnitude low 0000: Manual



// Sample Payload Below:
/*
{
    ID: '99999',
    Date: '1/7/2021',
    Time: '12:13:36.467',
    Status: '00 00',
    gpsLock: 1/0,
    error: 1/0,
    Frequency: '59.998001',
    'df/dt': '0',
    sRate: 30,
    Phasors: 
        {
            B345_BUSA_VS: {
                mag: '356953.7188',
                angle: '-179.048752'
            },
            B345_BUSB_VS: {
                mag: '356592.6563',
                angle: '-179.020111'
            }
        }
    }
  */

    /*
{
    ID: '99999',
    TS; 27283238923,
    Status: '00 00',
    sRate: 30,
    B345_BUSA_VS Mag: {'356953.7188', df/dt, Frequency},
    B345_BUSA_VS Angle: {'-179.048752', df/dt, Frequency}
}
  */


// // formats date properly to be processed in connector
// function arangeTime(input) {
//     // ex: 1/7/2021 to 2021-01-07
//     var parts = input.trim().split('/');
//     var month = null;
//     var day = null;
//     if (parts[0].length != 2) {
//         month = '0'.concat(parts[0]);
//     }
//     if (parts[1].length != 2) {
//         day = '0'.concat(parts[1]);
//     }
//     return parts[2].concat("-",month,"-",day)
// }



// loop1();




// loops through last two rows of small sample file
// adjust conditions when using large sample file
// see comments at bottom for an example payload to kinesis
// let loop1 = () => {
//     // i = 0
//     let keys = lines[0].split(",");
//     for (var i = 1; i < 3; i++) {
//         let row = lines[i].split(',');
//         let parameters = {
//             Data: JSON.stringify({
//                 ID: row[0],
//                 // converting data and time to unix time happens in connector
//                 Date: arangeTime(row[2]),
//                 Time: row[3],
//                 // converting status to gpsLock and error happens on publisher side
//                 Status: row[4],
//                 // Status returns 0 if gpsLock is on, but for our schema 1 means gpsLock is on, hence the XOR
//                 gpsLock: hex2bin(row[4].replace(/\s/g, ""))[13] ^ 1,      
//                 error: hex2bin(row[4].replace(/\s/g, ""))[14],
//                 Frequency: row[5],
//                 // calculating sRate happens on publisher side
//                 sRate: srate,
//                 dfdt: row[6],
//                 Phasors: JSON.stringify({
//                     [keys[7].split(' ')[0]]: {
//                         mag: row[7],
//                         angle: row[8]
//                     },
//                     [keys[9].split(' ')[0]]: {
//                         mag: row[9],
//                         angle: row[10]
//                     },
//                     [keys[11].split(' ')[0]]: {
//                         mag: row[11],
//                         angle: row[12]
//                     },
//                     [keys[13].split(' ')[0]]: {
//                         mag: row[13],
//                         angle: row[14]
//                     },
//                     [keys[15].split(' ')[0]]: {
//                         mag: row[15],
//                         angle: row[16]
//                     },
//                     [keys[17].split(' ')[0]]: {
//                         mag: row[17],
//                         angle: row[18]
//                     },
//                     [keys[19].split(' ')[0]]: {
//                         mag: row[19],
//                         angle: row[20]
//                     },
//                     [keys[21].split(' ')[0]]: {
//                         mag: row[21],
//                         angle: row[22]
//                     },
//                     [keys[23].split(' ')[0]]: {
//                         mag: row[23],
//                         angle: row[24]
//                     },
//                     [keys[25].split(' ')[0]]: {
//                         mag: row[25],
//                         angle: row[26]
//                     }
//                 })
//             }), 
//             PartitionKey: "key" + toString(i),
//             StreamName: "teststream"
//         }
//         kinesis.putRecord(parameters, function(err, data) {
//             if (err) {
//                 console.log('\n WARNING: Could not add record \n');
//                 console.log(err, err.stack); // an error occurred
//             }
//             else {  
//                 console.log('\n Record with following data added to stream: \n');  
//                 console.log(data);           // successful response
//             }
//         });
//     }
// }

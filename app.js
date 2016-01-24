var csv = require('fast-csv') // require fast-csv module
var fs = require('fs') // require the fs, filesystem module
var stream = fs.createReadStream("csv/customers.csv");
var stream2 = fs.createReadStream("csv/policies.csv");

csv
 .fromStream(stream, {headers : ["id", "firstName", "lastName", "dob"]})
 .on("data", function(data){
     console.log(data)
 })
 .on("end", function(){
     console.log("done");
 })

csv
 .fromStream(stream2, {headers : ["policy", "id", "insuranceType"]})
 .on("data", function(data){
     console.log(data)
 })
 .on("end", function(){
     console.log("done");
 })

 .pipe(csv.createWriteStream({headers: ["id", "firstName", "lastName", "policy", "insuranceType"]}))
 .pipe(fs.createWriteStream("report.csv"));

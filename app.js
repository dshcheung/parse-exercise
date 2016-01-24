// libraries to read csv
var csv              = require('fast-csv'); // require fast-csv module
var fs               = require('fs'); // require the fs, filesystem module

// initial valuables for customers
// errors already handled by fs library
var streamCustomers  = fs.createReadStream("csv/customers.csv");
var customersDone    = false;
var customers        = {};

// initial valuables for policies
// errors already handled by fs library
var streamPolicies   = fs.createReadStream("csv/policies.csv");
var policiesDone     = false;
var policies         = {};

// initializing the report csv file
var currentDate     = new Date();
var formattedDate   = currentDate.toISOString();
var reportCSV       = fs.createWriteStream("report-" + formattedDate + ".csv");

// start streaming data from customers.csv
csv.fromStream(streamCustomers, {headers : ["id", "firstName", "lastName", "dob"]})
  .on("data", function(data){
    customers[data.id] = {
      fullName: data.firstName + ' ' + data.lastName,
      dob: data.dob
    };
  })
  .on("end", function(){
    // because streaming is asynchronous, we don't know which stream will finish last
    // therefore we have two boolean variable representing each stream
    // and generateReport only when both stream is completed
    customersDone = true;
    generateReport();
  });

// start streaming data from policies.csv
csv.fromStream(streamPolicies  , {headers : ["policyNumber", "id", "insuranceType"]})
  .on("data", function(data){
    policies[data.id] = {
      policyNumber: data.policyNumber,
      insuranceType: data.insuranceType
    };
  })
  .on("end", function(){
    // because streaming is asynchronous, we don't know which stream will finish last
    // therefore we have two boolean variable representing each stream
    // and generateReport only when both stream is completed
    policiesDone = true;
    generateReport();
  });

function generateReport () {
  if (customersDone && policiesDone) {
    // an array to store reports
    var reports = [];

    // for each policies, generate a report
    for (var id in policies) {
      var customerID    = id;

      // retrieve customer object and relevent information from customers
      var customerOBJ   = customers[customerID];
      var fullName      = customerOBJ.fullName;

      // retrieve policy object and relevent information from policies
      var policyOBJ     = policies[customerID];
      var policyNumber  = policyOBJ.policyNumber;
      var insuranceType = policyOBJ.insuranceType;

      // Generate a array of report objects to be converted to csv
      reports.push({
        customerID: customerID,
        fullName: fullName,
        policyNumber: policyNumber,
        insuranceType: insuranceType
      });
    }

    // validate reports for quality control
    // generate csv when all test pass
    if (validateReport(reports)) {
      generateCSV(reports);
    }
  }
}

function validateReport (reports) {
  // test if any field in reports is missing
  // test if policy number contain 5 characters

  // for each test, console.log whether the test pass or fail, the reason, and for which customer
  // return true when all validation checks out otherwise return false
  return true;
}

function generateCSV (reports) {
  csv
    .write(reports, {headers: true})
    .on("end", function(){
      console.log("Successfully Generated Report!");
    })
    .pipe(reportCSV);
}
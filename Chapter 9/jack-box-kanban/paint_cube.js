#!/usr/bin/env node
// Author: Jeremy Nelson
// Licensed under GPLv3

var redis = require("redis"),
    client = redis.createClient(),
    client_subscriber = redis.createClient();


function PaintCubeWorkstation(redis) {
  var self = this;
  self.database = redis;
  self.input_bins = 0;
  self.output_bins = 0;
  self.database.on("subscribe", function(channel) {
    console.log("Subscribed to "+channel);
  });
  self.database.subscribe("kanban");
  self.database.subscribe("operations"); 
  self.database.on("message", function(channel, message) {
   if(channel === "operations") {
     if(message === "STOP") {
       self.database.unsubscribe();
       self.database.end();
       console.log("Stopping PaintCubeWorkstation");
       process.exit(1);
     }
   }
   if(channel === "kanban") {
     if(message === "PULL Paint") {
       console.log("Output bins " + self.output_bins);
       if(self.output_bins > 0) {
         client.publish("kanban", "READY Painted " + self.output_bins);
         self.output_bins -= 1;  
       } else {
         client.publish("kanban", "PULL Box");
  
       }
     }
     if(message.indexOf("READY Box") === 0){
       self.input_bins += 1;
       console.log("Input bin size " + self.input_bins)
       for(i = 0; i<=self.input_bins; i++) {
         // adds hinges and paints each box and adds to output bin
         console.log("\tAdds hinges and paints " + i);
         self.output_bins += 1;
       } 
       client.publish("kanban", "READY Painted " + self.output_bins);

     }
   }


  });
}


console.log("In Paint Cube Application");
var workstation = PaintCubeWorkstation(client_subscriber);


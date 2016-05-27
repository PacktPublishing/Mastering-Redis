/**
 * teabags.js - Node.js example of key expiration for Mastering Redis
 *
 * Licensed under the GPLv3 by Jeremy Nelson
 * Copyright 2015
 *
 */

var Redis = require('redis');
var client = redis.createClient();

function addTea(name, time, total_teabags) {
  redis.incr('global/tea').then(function(result) {
    var teaKey = "tea/" + result;
    redis.hmset(teaKey, {name: name, 
                         "brew-time": time,
                         "total-teabags": total_teabags}).then(
        function(result) {
          return teaKey;
        });
  });  
}

function addBox(tea_key) {
  redis.hget(tea_key, "total-teabags").then(function(result) {
    var tea_bags = [];
    for(i=0; i <= result; i++) {  
      tea_bags.push(i);
    }
    redis.incr('global/'+tea_key+'/box').then(function(result) {
        var box_key = tea_key +'/box/' + result;
        redis.sadd(box_key, tea_bags); 
        return box_key;
    });
  });
}


function addAllTeas() {
  var tea1 = addTea("Earl Grey", 5, 15);
  console.log("Tea 1 is " + tea1);
  var tea1_box1 = addBox(tea1);
  var tea2 = addTea("Lavender Mint", 3, 20);
  var tea2_box1 = addBox(tea2);
  var tea3 = addTea("Pepperment Punch", 4, 20);
  var tea3_box1 = addBox(tea3);
  return true;
}

addAllTeas();

var N3 = require('n3');
var fs = require('fs');
var net = require('net');

var mandela_ttl = '';

fs.readFile('nelson-mandela.ttl', 'utf8', function(error, data) {
  if (error) {
    return console.log(error);
  }
  mandela_ttl = data;
});


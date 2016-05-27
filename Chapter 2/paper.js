var nohm = require('nohm').Nohm;
var redis = require('redis').createClient();

nohm.setClient(redis);
nohm.setPrefix('paper');

nohm.model('Stationary', {
  properties: {
    color: {
      type: 'string',
      unique: false,
      validations: [
        'notEmpty'
      ]
    },
    height: {
      type: 'string',
      unique: false
    },
    sheets: {
      type: 'integer',
      defaultValue: 20
    },
    width: {
      type: 'string',
      unique: false
    }  
  }
});

nohm.model('Offer', {
  properties: {
    inventoryLevel: {
      type: 'integer',
      unique: false
    },
    price: {
      type: 'float',
      unique: false
    },
    priceCurrency: {
      type: 'string',
      unique: false,
      defaultValue: 'USD'
    }
  }
});

nohm.model('Order', {
  properties: {
    orderDate: {
      type: 'datetime'
    }
  
  },
  idGenerator: 'increment',
});


var blue_stationary = nohm.factory('Stationary');
var red_stationary = nohm.factory('Stationary'); 
var offer4blue = nohm.factory('Offer');
var offer4red = nohm.factory('Offer')


function Chapter2Example() {
  /*
  


  */   
  blue_stationary.p({
    color: 'blue',
    height: '40 cm',
    width: '30 cm'
  });

  offer4blue.p({
    inventoryLevel: 250,
    price: 10
  });

  offer4blue.link(blue_stationary, 'itemOffered');

  blue_stationary.save(function(err) { 
    if(err) { 
     console.log('Error', err); 
    } else {
     console.log("Saved stationary!");
   }
  });

  offer4blue.save(function(err) {offer4blue
    if(err) { console.log('Error with offer4blue'); }
    else { console.log('offer4blue saved'); }});

  red_stationary.p({
    color: 'red',
    height: '45 cm',
    sheets: 15,
    width: '45 cm',
  });
 
  offer4red.p({
    inventoryLevel: 50,
    price: 15
  });

  offer4red.link(red_stationary, 'itemOffered');


  red_stationary.save(function(err) { 
    if(err) { 
      console.log('Error', err); 
    } else {
    console.log("Saved stationary!");
   }
  });
  
  offer4red.save(function(err) { console.log("Saved offer4red"); });
}

function Sale() {
  var order = nohm.factory('Order');
  var now = new Date();
  order.p({ orderDate: now });
  order.link(offer4blue, 'offer');
  order.save(function(err) { console.log("Saved blude order"); }); 
}

var args = process.argv.slice(2);
var action = args[0];

if(action === 'example') {
  console.log("Running Chapter Two Example");
  Chapter2Example();
  Sale();
}
if(action === 'sale') {
  console.log("Running Chapter Two Sales");
  Sale();
}

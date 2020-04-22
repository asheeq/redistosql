var express = require("express");
var bodyParser = require('body-parser');
var app = express();
var dateFormat = require('dateformat');
var redis = require('redis');
var client = redis.createClient();
const asyncRedis = require("async-redis");
const asyncRedisClient = asyncRedis.decorate(client);
client.on('connect', function() {
    console.log('AsyncRedis client connected');
});
client.on('error', function (err) {
    console.log('Something went wrong ' + err);
});
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
Date.prototype.toUnixTime = function() { return this.getTime()/1000|0 };
Date.time = function() { return new Date().toUnixTime(); }
const Enum = require('enum');
const types = new Enum(['read', 'write', 'execute']);
app.post('/api/event', (req, res) => {
    if(!Number.parseInt(req.body.eventID)) {
      return res.status(400).send({
        status: '400 Bad Request',
        message: 'Must provide an integer.'
      });
    } else if(!(req.body.eventType in types)) {
      return res.status(400).send({
        status: '400 Bad Request',
        message: 'Invalid eventType'
      });
    } else if((req.body.eventMessage).length > 1024){
      return res.status(400).send({
          status: '400 Bad Request',
          message: 'Max-length is 1024.'
      });  
    } else if(!dateFormat(req.body.eventDate)){
      return res.status(400).send({
          status: '400 Bad Request',
          message: 'invalid date format.'
      });
    }
   var value = req.body.eventID+"|"+req.body.eventType+"|"+req.body.eventMessage+"|"+req.body.eventDate;
   asyncRedisClient.rpush([Date.time(),value],function(err,res){
      if(err){
        return res.status(501).send({
          status: '501 Not Implemented',
          message: 'error found.'
        })
      }
   });
   return res.status(201).send({
    status: '200 OK',
    message: 'event added successfully.'
  })  
});

app.listen(3000, () => {
 console.log("Server running on port 3000");
});
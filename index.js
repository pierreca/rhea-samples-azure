var rhea = require('rhea');
var uuid = require('uuid');

var hubName = '<hub name>';
var hostName = hubName + '.azure-devices.net';
var deviceId = '<device-id>';
var deviceSas = '<deviceSas>';

rhea.on('connection_open', (context) => {
 console.log('opened');
  context.connection.on('error', (context) => {
    console.log('connection error');
  });

  var receiver = context.connection.open_receiver( {
    source: '/devices/' + deviceId + '/messages/devicebound',
    autoaccept: false // true by default
  });
  receiver.on('receiver_open', (context) => {
    console.log('receiver opened');
  });

  receiver.on('message', (context) => {
    console.log('message received');
    console.log(context.message.body.content.toString());
    context.delivery.accept();
    // context.delivery.reject();
    // context.delivery.release();
  });

  // var msgqueue = [];
  // var sender = context.connection.open_sender('/devices/' + deviceId + '/messages/events');
  // sender.on('sender_open', (context) => {
  //   console.log('sender opened');
  //   msgqueue.push(sender.send({
  //     id: uuid.v4(),
  //     body: 'foo'
  //   }));
  //   msgqueue.push(sender.send({
  //     id: uuid.v4(),
  //     body: 'bar'
  //   }));

  //   sender.on('accepted', (context) => {
  //     for (var i = 0; i < msgqueue.length; i++) {
  //       if (msgqueue[i] === context.delivery) {
  //         console.log('accepted');
  //       } else {
  //         console.log('nope');
  //       }
  //     }
  //   })
  // });

  // sender.on('error', (context) => {
  //   console.log('sender error');
  // });

  // sender.on('sender_close', (context) => {
  //   console.log('sender closed');
  // });

});

rhea.on('error', (context) => {
  console.log('error');
});

rhea.on('disconnected', (context) => {
  console.log('disconnected');
});

var connection  = rhea.connect({
  host: hostName,
  hostname: hostName,
  username: deviceId + '@sas.' + hubName,
  password: deviceSas,
  port: 5671,
  transport: 'tls',
  reconnect: false
});


var rhea = require('rhea');
var uuid = require('uuid');
var uuidBuffer = require('uuid-buffer');

var hubName = '<hub name>';
var hostName = hubName + '.azure-devices.net';
var deviceId = '<device-id>';
var deviceSas = '<deviceSas>';

rhea.on('connection_open', (context) => {
 console.log('opened');
  context.connection.on('error', (context) => {
    console.log('connection error');
  });

  /**
   * D2C
   */
  var msgqueue = [];
  var d2c_sender = context.connection.open_sender( {
    target: '/devices/' + deviceId + '/messages/events',
    properties: {
      'com.microsoft:client-version': 'rhea-test'
    }
  });
  d2c_sender.on('sender_open', (context) => {
    console.log('d2c sender opened');
    msgqueue.push(d2c_sender.send({
      message_id: uuid.v4(),
      body: 'foo',
      application_properties: {
        apppropkey: 'apppropvalue'
      },
      message_annotations: {
        status: 201
      }
    }));

    d2c_sender.on('accepted', (context) => {
      for (var i = 0; i < msgqueue.length; i++) {
        if (msgqueue[i] === context.delivery) {
          console.log('accepted');
        } else {
          console.log('nope');
        }
      }
    })
  });

  d2c_sender.on('error', (context) => {
    console.log('sender error');
  });

  d2c_sender.on('sender_close', (context) => {
    console.log('sender closed');
  });

  /**
   * C2D
   */
  var c2d_receiver = context.connection.open_receiver( {
    source: '/devices/' + deviceId + '/messages/devicebound',
    autoaccept: false // true by default
  });
  c2d_receiver.on('receiver_open', (context) => {
    console.log('c2d receiver opened');
  });

  c2d_receiver.on('message', (context) => {
    console.log('message received');
    console.log(context.message.body.content.toString());
    context.delivery.accept();
    // context.delivery.reject();
    // context.delivery.release();
  });

  /**
   * Direct Methods
   */
  var method_response_queue = [];
  var method_sender = context.connection.open_sender( {
    target: '/devices/' + deviceId + '/methods/devicebound',
    properties: {
      'com.microsoft:client-version': 'rhea-test',
      'com.microsoft:api-version': '2017-06-30',
      'com.microsoft:channel-correlation-id': deviceId
    }
  });
  method_sender.on('sender_open', (context) => {
    console.log('method sender opened');

    method_sender.on('accepted', (context) => {
      for (var i = 0; i < method_response_queue.length; i++) {
        if (method_response_queue[i] === context.delivery) {
          console.log('accepted');
        } else {
          console.log('nope');
        }
      }
    })
  });

  method_sender.on('error', (context) => {
    console.log('sender error');
  });

  method_sender.on('sender_close', (context) => {
    console.log('sender closed');
  });

  var method_receiver = context.connection.open_receiver( {
    source: '/devices/' + deviceId + '/methods/devicebound',
    properties: {
      'com.microsoft:client-version': 'rhea-test',
      'com.microsoft:api-version': '2017-06-30',
      'com.microsoft:channel-correlation-id': deviceId
    }
  });
  method_receiver.on('receiver_open', (context) => {
    console.log('method receiver opened');
  });

  method_receiver.on('message', (context) => {
    console.log('method request received');
    console.log('method name: ' + context.message.application_properties['IoThub-methodname']);
    console.log('payload: ' + context.message.body.content.toString());
    console.log('request id: ' + context.message.correlation_id);
    console.log('uuidbuffer: ' + uuidBuffer.toString(context.message.correlation_id));
    method_response_queue.push(method_sender.send({
      // TODO: need to figure out if we can "force" the encoding of the correlation_id as a UUID. Right now it's encoded as a string and IoT Hub rejects it.
      correlation_id: uuidBuffer.toString(context.message.correlation_id),
      body: {
        'foo': 'bar'
      },
      application_properties: {
        'IoThub-status': 200
      }
    }));
  });
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


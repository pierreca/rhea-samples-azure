const url = require('url');
const rhea = require('rhea');
const amqp = require('./promises');
const uuid = require('uuid');
const crypto = require('crypto');

const connectionString = process.argv[2];

const parsedCS = amqp.parseConnectionString(connectionString);
const eventHubName = parsedCS.EntityPath || process.argv[3];
const hostName = "testeh12.servicebus.windows.net";

function messageHandler(result) {
  console.log('result.message: ', result.message);
  //console.dir(result.delivery, {depth: 2});
  let bodyStr = result.message.body.toString();
  console.log('received(' + myIdx + '): ', bodyStr);

  try {
    JSON.parse(bodyStr);
  } catch (e) {
    //console.log(e);
  }
  result.delivery.update(undefined, rhea.message.accepted().described());
}

function errorHandler(rx_err) {
  console.warn('==> RX ERROR: ', rx_err);
}

function stringToSign(resourceUri, expiry) {
  return resourceUri + '\n' + expiry;
}

function hmacHash(password, stringToSign) {
  let hmac = crypto.createHmac('sha256', new Buffer(password, 'base64'));
  hmac.update(stringToSign, 'utf8');
  return hmac.digest('base64');
}

function createSasToken(resourceUri, keyName, key, expiry) {
  console.log('expiry being set to: ', new Date(expiry * 1000).toString());
  resourceUri = encodeURIComponent(resourceUri);
  keyName = encodeURIComponent(keyName);
  let sig = encodeURIComponent(hmacHash(key, stringToSign(resourceUri, expiry)));
  let sasToken = `SharedAccessSignature sr=${resourceUri}&sig=${sig}&se=${expiry}&skn=${keyName}`;
  return sasToken;
}

async function cbsAuth() {
  return new Promise(async function (resolve, reject) {
    const endpoint = '$cbs';
    const replyTo = 'cbs';
    const resourceUri = `${parsedCS.Endpoint}${eventHubName}`;
    const audience = resourceUri;
    const sasToken = createSasToken(resourceUri, parsedCS.SharedAccessKeyName, parsedCS.SharedAccessKey, parseInt((Date.now() + 3600000) / 1000).toString());
    console.log('sasToken: ', sasToken);

    const request = {
      body: sasToken,
      properties: {
        message_id: uuid.v4(),
        reply_to: replyTo,
        to: endpoint,
      },
      application_properties: {
        operation: "put-token",
        name: audience,
        type: "servicebus.windows.net:sastoken"
      }
    };
    console.log('request: ', request);
    //const rxopt = { name: replyTo, target: { address: replyTo }};

    const connection = await amqp.fromConnectionString(connectionString, { useSaslAnonymous: true });
    console.log('connected');

    const session = await amqp.createSession(connection);

    console.log('got sessions');

    const [sender, receiver] = await Promise.all([
      amqp.createSender(session, endpoint, {}),
      amqp.createReceiver(session, endpoint)
    ]);

    receiver.on('message', ({ message, delivery }) => {
      console.log('rx: ', message);
      console.log('cbs response received');
      //console.log(message.body.content.toString());
      delivery.update(undefined, rhea.message.accepted().described());
      process.exit();
    });

    const delivery = sender.send(request);
    //console.log('delivery: ', delivery);
    console.log('sent message');
  }.bind(this));
}

cbsAuth().then((res) => {
  console.log(res);
  process.exit();
}).catch(err => {
  console.error(err);
  console.log(err.stack);
  process.exit(1);
});
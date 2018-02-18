const url = require('url');
const rhea = require('rhea');
const amqp = require('./promises');
const uuid = require('uuid');
const crypto = require('crypto');

const connectionString = process.argv[2];

const parsedCS = amqp.parseConnectionString(connectionString);
const eventHubName = parsedCS.EntityPath || process.argv[3];

var connection;

function createSasToken(resourceUri, keyName, key, expiry) {
  console.log('expiry being set to: ', new Date(expiry * 1000).toString());
  resourceUri = encodeURIComponent(resourceUri);
  keyName = encodeURIComponent(keyName);
  let stringToSign = resourceUri + '\n' + expiry;
  let sig = encodeURIComponent(crypto.createHmac('sha256', key).update(stringToSign, 'utf8').digest('base64'));
  console.log('#####sig: ', sig);
  let sasToken = `SharedAccessSignature sr=${resourceUri}&sig=${sig}&se=${expiry}&skn=${keyName}`;
  return sasToken;
}

async function cbsAuth() {
  return new Promise(async function (resolve, reject) {
    const endpoint = '$cbs';
    const replyTo = 'cbs';
    const resourceUri = `${parsedCS.Endpoint}${eventHubName}`;
    const audience = resourceUri;
    const sasToken = createSasToken(resourceUri, parsedCS.SharedAccessKeyName, parsedCS.SharedAccessKey, Math.floor((Date.now() + 3600000) / 1000).toString());
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

    connection = await amqp.fromConnectionString(connectionString, { useSaslAnonymous: true });
    console.log('connected');

    const session = await amqp.createSession(connection);

    console.log('got sessions');

    const [sender, receiver] = await Promise.all([
      amqp.createSender(session, endpoint, {}),
      amqp.createReceiver(session, endpoint)
    ]);

    receiver.on('message', ({ message, delivery }) => {
      console.log('message: ', message);
      console.log('cbs response received');
      delivery.update(undefined, rhea.message.accepted().described());
    });

    const delivery = sender.send(request);
    //console.log('delivery: ', delivery);
    console.log('sent message');
  }.bind(this));
}

async function run() {
  await cbsAuth();
  process.exit();
}

run().catch(err => {
  console.error(err);
  console.log(err.stack);
  process.exit(1);
});
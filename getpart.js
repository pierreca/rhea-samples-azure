const url = require('url');
const rhea = require('rhea');
const amqp = require('./promises');
const uuid = require('uuid');


const connectionString = process.argv[2];
const eventHubName = "test1"; //process.argv[3];


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

async function getPartitionIds() {
  const endpoint = '$management';
  const replyTo = uuid.v4();
  const request = {
    body: Buffer.from(JSON.stringify([])),
    properties: {
      messageId: uuid.v4(),
      replyTo: replyTo
    },
    applicationProperties: {
      operation: "READ",
      name: eventHubName,
      type: "com.microsoft:eventhub"
    }
  };
  const rxopt = {target: { address: replyTo }};

  const connection = await amqp.fromConnectionString(connectionString);
  console.log('connected');

  const [senderSession, receiverSession] = await Promise.all([
    amqp.createSession(connection),
    amqp.createSession(connection)
  ]);

  console.log('got sessions');

  const [sender, receiver] = await Promise.all([
    amqp.createSender(senderSession, endpoint, {}),
    amqp.createReceiver(receiverSession, endpoint, rxopt)
  ]);

  receiver.on('message', ({ message, delivery }) => {
    console.log('rx: ', JSON.parse(message.body.toString()));
    const code = message.applicationProperties['status-code'];
    const desc = message.applicationProperties['status-description'];
    if (code === 200) {
      return Promise.resolve(message.body.partition_ids);
    }
    else if (code === 404) {
      return Promise.reject(desc);
    }
    delivery.update(undefined, rhea.message.accepted().described()); // Complete
    // delivery.update(undefined, rhea.message.rejected().described()); // DeadLetter
    // delivery.update(undefined, rhea.message.modified().described({ undeliverable_here: true })); // Abandon
    // delivery.update(undefined, rhea.message.released().described()); // Defer
  });

  const delivery = sender.send(request);
  //console.log('delivery: ', delivery);
  console.log('sent message');
}

getPartitionIds().catch(err => {
  console.error(err);
  process.exit(1);
});
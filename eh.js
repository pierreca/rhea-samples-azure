const url = require('url');
const rhea = require('rhea');
const amqp = require('./promises');
const uuid = require('uuid');


const connectionString = process.argv[2];
const eventHubName = "test1"; //process.argv[3];
const numPartitions = 11; //process.argv[4];

function range(begin, end) {
  return Array.apply(null, new Array(end - begin)).map(function (_, i) { return i + begin; });
}

const rxIds = range(0, numPartitions);

function messageHandler(myIdx, result) {
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

function errorHandler(myIdx, rx_err) {
  console.log('myIdx: ', myIdx);
  console.warn('==> RX ERROR: ', rx_err);
}

async function createPartitionReceiver(receiverSession, rcvAddr, curIdx, options) {
  return amqp.createReceiver(receiverSession, rcvAddr + curIdx, options)
    .then(function (receiver) {
      receiver.on('message', messageHandler.bind(null, curIdx));
      receiver.on('error', errorHandler.bind(null, curIdx));
      return Promise.resolve();
    });
}

async function getPartitionIds() {
  const endpoint = '$management';
  const replyTo = uuid.v4();
  const request = {
    body: [],
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
}

async function main() {
  const connection = await amqp.fromConnectionString(connectionString);
  console.log('connected');

  const [senderSession, receiverSession] = await Promise.all([
    amqp.createSession(connection),
    amqp.createSession(connection)
  ]);

  console.log('got sessions');

  const sendAddr = eventHubName;
  const recvAddr = eventHubName + '/ConsumerGroups/$default/Partitions/';

  const [sender, unused] = await Promise.all([
    amqp.createSender(senderSession, sendAddr, {}),
    createPartitionReceiver(receiverSession, recvAddr, 0, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 1, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 2, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 3, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 4, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 5, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 6, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 7, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 8, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 9, { autoaccept: false }),
    createPartitionReceiver(receiverSession, recvAddr, 10, { autoaccept: false })
  ]);

  console.log('created sender and receiver');

  const message = {
    body: Buffer.from(JSON.stringify({
      hello: 'world'
    })),
    message_annotations: {
      'x-opt-partition-key': 'pk1234'
    }
  };

  const delivery = sender.send(message);
  //console.log('delivery: ', delivery);
  console.log('sent message');
  // TODO: handle errors after setup
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
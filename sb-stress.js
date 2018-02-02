#! /usr/bin/env node

const os = require('os');
const yargs = require('yargs');
const rhea = require('rhea');

const amqp = require('./promises');
const fromConnectionString = require('./sb');

const args = yargs
    .usage('$0 <connectionString> <queueName>')
    .options({
        count: {
            alias: 'c',
            number: true,
            describe: 'The number of messages to send in each iteration',
            default: 2e6 // 2 million
        },
        duration: {
            alias: 'd',
            number: true,
            describe: 'The period of time (in milliseconds) to allow each iteration to run',
            default: 1000 * 60 * 60 * 2 // 2 hours
        },
        iterations: {
            alias: 'i',
            number: true,
            describe: 'The number of consecutive iterations to execute before completing',
            default: 12
        },
        rate: {
            alias: 'r',
            number: true,
            describe: 'The rate at which messages should be sent in messages per second',
            default: 500
        }
    })
    .demandCommand(2)
    .argv;

if (args.count / args.rate > args.duration / 1000) {
    console.error(`Not enough time to send all messages. The message send rate (${args.rate}/s) must be large enough that all messages (${args.count}) can be sent within the configured duration (${args.duration} ms)`);
    process.exit(1);
}

const messagesByIteration = [];
const messagesByCompletedIteration = [];

run().catch(err => {
    console.error('Encountered an unexpected error:');
    console.error(err);
    process.exit(1);
});

async function run() {
    const connection = await fromConnectionString(args._[0]);

    const [senderSession, receiverSession] = await Promise.all([
        amqp.createSession(connection),
        amqp.createSession(connection)
    ]);

    const [sender, receiver] = await Promise.all([
        amqp.createSender(senderSession, args._[1], {}),
        amqp.createReceiver(receiverSession, args._[1], {
            autoaccept: false
        })
    ]);

    const send = amqp.trackSends(sender);

    receiver.on('message', ({ message, delivery }) => {
        const body = JSON.parse(message.body.toString());
        if (body.iteration) {
            messagesByIteration[body.iteration].received++;
        }
        delivery.update(undefined, rhea.message.accepted().described()); // Complete
    });

    for (let i = 0; i < args.iterations; i++) {
        messagesByIteration[i] = { sent: 0, failedSend: 0, received: 0 };

        console.log('================================================');
        console.log(`STARTING ITERATION ${i}`);
        console.log('================================================');
        await runIteration(i, send);
        messagesByCompletedIteration[i] = JSON.parse(JSON.stringify(messagesByIteration[i]));
        console.log('================================================');
        console.log(`FINISHED ITERATION ${i}`);
        console.log(`Sent: ${messagesByCompletedIteration[i].sent}`);
        console.log(`Received: ${messagesByCompletedIteration[i].received}`);
        console.log(`Send Percentage: ${messagesByCompletedIteration[i].sent * 100 / args.count}%`);
        console.log(`Receive Percentage: ${messagesByCompletedIteration[i].received * 100 / messagesByCompletedIteration[i].sent}%`);
        console.log('================================================');
        console.log('\n');
    }
}

async function runIteration(iteration, send) {
    const startTime = Date.now();

    const messagesPerBatch = args.rate / 10;
    let messageIndex = 0;
    let sentCount = 0;
    const sendInterval = setInterval(() => {
        const startIndex = messageIndex;
        for (; messageIndex < startIndex + messagesPerBatch && messageIndex < args.count; messageIndex++) {
            send({ body: Buffer.from(JSON.stringify({ messageIndex, iteration })) })
                .then(() => messagesByIteration[iteration].sent++)
                .catch(() => messagesByIteration[iteration].failedSend++);
        }

        if (messageIndex >= args.count) {
            clearInterval(sendInterval);
        }
    }, 100);

    const statsInterval = setInterval(() => {
        console.log(`Time: ${Date.now() - startTime}\tSent: ${messagesByIteration[iteration].sent}\tReceived: ${messagesByIteration[iteration].received}\tCPU: ${os.loadavg()[0]}\tMem: ${process.memoryUsage().heapUsed}`)
    }, 60000);

    return new Promise((resolve, reject) => {
        setTimeout(() => {
            clearInterval(sendInterval);
            clearInterval(statsInterval);
            resolve();
        }, args.duration);
    });
}
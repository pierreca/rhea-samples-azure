const url = require('url');
const rhea = require('rhea');
const amqp = require('./promises');

async function fromConnectionString(connectionString) {
    const parsed = connectionString.split(';').reduce((acc, part) => {
        const splitIndex = part.indexOf('=');
        return {
            ...acc,
            [part.substring(0, splitIndex)]: part.substring(splitIndex + 1)
        };
    }, {});

    const queue = process.argv[3]

    return await amqp.connect({
        transport: 'tls',
        host: url.parse(parsed.Endpoint).hostname,
        hostname: url.parse(parsed.Endpoint).hostname,
        username: parsed.SharedAccessKeyName,
        password: parsed.SharedAccessKey,
        port: 5671,
        reconnect_limit: 100
    });
}

module.exports = fromConnectionString;

if (require.main === module) {
    async function main() {
        const connectionString = process.argv[2];
        const queue = process.argv[3];
        
        const connection = await fromConnectionString(connectionString);

        console.log('connected');

        const [senderSession, receiverSession] = await Promise.all([
            amqp.createSession(connection),
            amqp.createSession(connection)
        ]);

        console.log('got sessions');

        const [sender, receiver] = await Promise.all([
            amqp.createSender(senderSession, queue, {}),
            amqp.createReceiver(receiverSession, queue, {
                autoaccept: false
            })
        ]);

        console.log('created sender and receiver');

        const send = amqp.trackSends(sender);

        receiver.on('message', ({ message, delivery }) => {
            console.log('rx: ', JSON.parse(message.body.toString()));
            delivery.update(undefined, rhea.message.accepted().described()); // Complete
            // delivery.update(undefined, rhea.message.rejected().described()); // DeadLetter
            // delivery.update(undefined, rhea.message.modified().described({ undeliverable_here: true })); // Abandon
            // delivery.update(undefined, rhea.message.released().described()); // Defer
        });

        await send({
            body: Buffer.from(JSON.stringify({
                hello: 'world'
            }))
        });

        console.log('sent message');

        // TODO: handle errors after setup
    }

    main().catch(err => {
        console.error(err);
        process.exit(1);
    });
}
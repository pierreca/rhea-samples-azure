const rhea = require('rhea');

async function connect(options) {
    return new Promise((resolve, reject) => {
        const connection = rhea.connect(options);

        function onOpen(context) {
            connection.removeListener('connection_open', onOpen);
            connection.removeListener('connection_close', onClose);
            connection.removeListener('disconnected', onClose);
            resolve(connection);
        }

        function onClose(err) {
            connection.removeListener('connection_open', onOpen);
            connection.removeListener('connection_close', onClose);
            connection.removeListener('disconnected', onClose);
            reject(err);
        }

        connection.once('connection_open', onOpen);
        connection.once('connection_close', onClose);
        connection.once('disconnected', onClose);
    });
}

async function createSession(connection) {
    return new Promise((resolve, reject) => {
        const session = connection.create_session();

        function onOpen(context) {
            session.removeListener('session_open', onOpen);
            session.removeListener('session_close', onClose);
            resolve(session);
        }

        function onClose(err) {
            session.removeListener('session_open', onOpen);
            session.removeListener('session_close', onClose);
            reject(err);
        }

        session.once('session_open', onOpen);
        session.once('session_close', onClose);

        session.begin();
    });
}

async function createSender(session, path, options) {
    return new Promise((resolve, reject) => {
        const sender = session.attach_sender(path);

        function onOpen(context) {
            sender.removeListener('sendable', onOpen);
            sender.removeListener('sender_close', onClose);
            resolve(sender);
        }

        function onClose(err) {
            sender.removeListener('sendable', onOpen);
            sender.removeListener('sender_close', onClose);
            reject(err);
        }

        sender.once('sendable', onOpen);
        sender.once('sender_close', onClose);
    });
}

async function createReceiver(session, path, options) {
    return new Promise((resolve, reject) => {
        const receiver = session.attach_receiver(path);

        function onOpen(context) {
            receiver.removeListener('receiver_open', onOpen);
            receiver.removeListener('receiver_close', onClose);
            resolve(receiver);
        }

        function onClose(err) {
            receiver.removeListener('receiver_open', onOpen);
            receiver.removeListener('receiver_close', onClose);
            reject(err);
        }

        receiver.once('receiver_open', onOpen);
        receiver.once('receiver_close', onClose);
    });
}

function trackSends(sender) {
    const pendingMessages = new Map();

    function onSuccess(context) {
        const deliveryTag = context.delivery.tag.toString('hex');
        if (pendingMessages.has(deliveryTag)) {
            pendingMessages.get(deliveryTag).resolve();
            pendingMessages.delete(deliveryTag);
        } else {
            console.warn(`Tried to settle unknown message with tag ${deliveryTag}`);
        }
    }

    function onFailure(context) {
        const deliveryTag = context.delivery.tag.toString('hex');
        if (pendingMessages.has(deliveryTag)) {
            pendingMessages.get(deliveryTag).reject(new Error('Send failed'));
            pendingMessages.delete(deliveryTag);
        } else {
            console.warn(`Tried to settle unknown message with tag ${deliveryTag}`);
        }
    }

    sender.on('accepted', onSuccess);
    sender.on('released', onFailure);
    sender.on('rejected', onFailure);
    sender.on('modified', onFailure);

    return async (message) => {
        const delivery = sender.send(message);
        const deliveryTag = delivery.tag.toString('hex');

        return new Promise((resolve, reject) => {
            // TODO: timeout
            pendingMessages.set(deliveryTag, { resolve, reject });
        });
    };
}

module.exports = { connect, createSession, createSender, createReceiver, trackSends };
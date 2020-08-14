const cluster = require('cluster')
const mqemitter = require('mqemitter-mongodb')
const mongoPersistence = require('aedes-persistence-mongodb')

const MONGO_URL = 'mongodb+srv://mqtts:6jrVnShzc6B9o9x9@as-mqtts-persistence-db.adj9k.mongodb.net/mqtts?retryWrites=true&w=majority'

function startAedes() {
    const port = 1883

    const aedes = require('aedes')({
        id: 'BROKER_' + cluster.worker.id,
        mq: mqemitter({
            url: MONGO_URL
        }),
        persistence: mongoPersistence({
            url: MONGO_URL
            // Optional ttl settings
            //   ttl: {
            //     packets: 300, // Number of seconds
            //     subscriptions: 300
            //   }
        })
    })

    const server = require('net').createServer(aedes.handle)

    server.listen(port, function () {
        console.log('Aedes listening on port:', port)

    })

    aedes.on('subscribe', function (subscriptions, client) {
        console.log('MQTT client \x1b[32m' + (client ? client.id : client) +
            '\x1b[0m subscribed to topics: ' + subscriptions.map(s => s.topic).join('\n'), 'from broker', aedes.id)
    })

    aedes.on('unsubscribe', function (subscriptions, client) {
        console.log('MQTT client \x1b[32m' + (client ? client.id : client) +
            '\x1b[0m unsubscribed to topics: ' + subscriptions.join('\n'), 'from broker', aedes.id)
    })

    // fired when a client connects
    aedes.on('client', function (client) {
        console.log('Client Connected: \x1b[33m' + (client ? client.id : client) + '\x1b[0m', 'to broker', aedes.id)
    })

    // fired when a client disconnects
    aedes.on('clientDisconnect', function (client) {
        console.log('Client Disconnected: \x1b[31m' + (client ? client.id : client) + '\x1b[0m', 'to broker', aedes.id)
    })

    // fired when a message is published
    aedes.on('publish', async function (packet, client) {
        console.log('Client \x1b[31m' + (client ? client.id : 'BROKER_' + aedes.id) + '\x1b[0m has published', packet.payload.toString(), 'on', packet.topic, 'to broker', aedes.id)
    })

    // setInterval(() => {
    //     aedes.publish({ topic: 'aedes/hello', payload: "I'm broker " + aedes.id })
    // }, 10000);
}

if (cluster.isMaster) {
    const numWorkers = require('os').cpus().length
    // console.log('CPU length= ', numWorkers)
    for (let i = 0; i < numWorkers; i++) {
        cluster.fork()
    }

    cluster.on('online', function (worker) {
        // console.log('Worker ' + worker.process.pid + ' is online')
    })

    cluster.on('exit', function (worker, code, signal) {
        // console.log('Worker ' + worker.process.pid + ' died with code: ' + code + ', and signal: ' + signal)
        // console.log('Starting a new worker')
        cluster.fork()
    })
} else {
    startAedes()
}

const { Kafka } = require('kafkajs');
const WebSocket = require('ws');

const fs = require('fs');
const path = require('path');

const kafka = new Kafka({
  clientId: 'location-consumer',
  brokers: ['kafka-321cbf90-location.a.aivencloud.com:15944'],
  ssl: {
    ca : [fs.readFileSync(path.join(__dirname, 'ca.pem'), 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'avnadmin',
    password: 'AVNS_Y_5WSiNJAWxVC59hn6L',
  },
});

const consumer = kafka.consumer({ groupId: '1' });

async function consumeMessagess(ws) {
  await consumer.connect();
  await consumer.subscribe({ topic: 'location', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const locationData = JSON.parse(message.value.toString());

      ws.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify(locationData));
        }
      });
    },
  });
}

module.exports.consumeMessagess = consumeMessagess;

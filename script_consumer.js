// server.js
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { Kafka } = require('kafkajs');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

const { consumeMessages } = require('./client1');
const { consumeMessagess } = require('./client2');
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const path = require('path');

app.use(express.static(path.join(__dirname , 'public')));

const port = 9000;
  server.listen(port, () => {
    console.log(`Server is running`);
});

consumeMessages(wss);
consumeMessagess(wss);


// --------------------------------------------------------------------------------------

const kafkaBrokerUrl = 'kafka-321cbf90-location.a.aivencloud.com:15944';  

const kafkaP = new Kafka({
  clientId: 'location-producer',
  brokers: [kafkaBrokerUrl],
  ssl: {
    ca : [fs.readFileSync(path.join(__dirname, 'ca.pem'), 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'avnadmin',
    password: 'AVNS_Y_5WSiNJAWxVC59hn6L',
  },
});

let producer = null;

  function showError(error) {
    let errorMessage = 'Unknown error';

    switch (error.code) {
        case error.PERMISSION_DENIED:
            errorMessage = 'User denied the request for Geolocation.';
            break;
        case error.POSITION_UNAVAILABLE:
            errorMessage = 'Location information is unavailable.';
            break;
        case error.TIMEOUT:
            errorMessage = 'The request to get user location timed out.';
            break;
        case error.UNKNOWN_ERROR:
            errorMessage = 'An unknown error occurred.';
            break;
    }
}
async function connectToKafka() {
  try {
    if (producer) {
      return;
    }
    producer = kafkaP.producer();
    await producer.connect();
    console.log('Connected to Kafka');
  } catch (error) {
    showError(error);
  }
}

const run = async (latitude , longitude , userId) => {
  try {
      // await producer.connect() 
      await producer.send({
          topic: 'consumerLocation',
          messages: [
              { 
                value: JSON.stringify({ latitude , longitude , userId }) 
              },
          ],
      });
  } catch (error) {
      showError(error);
  }
}

const run2 = async (userId , event) => {
  try {
      // await producer.connect() 
      await producer.send({
          topic: 'consumerFinal',
          messages: [
              { 
                value: JSON.stringify({ userId , event }) 
              },
          ],
      });
  } catch (error) {
      showError(error);
  }
}

async function produceMessagess(wss) {
  await connectToKafka();
  wss.on('connection', (ws) => {
    ws.on('message', async (message) => {
        const location = JSON.parse(message);
        if (location.event == 'closing')
        {
          const userId = location.userId;
          const event = location.event;
          await run2(userId , event).catch(console.error)
        }
        else
        {
          const latitude = location.latitude;
          const longitude = location.longitude;
          const userId = location.userId;
          await run(latitude , longitude , userId).catch(console.error);
        }
    });
  });
}

produceMessagess(wss);

// ------------------------------------------------------------------------------------------------------------

app.get('/', (req, res) => {
  const userId = uuidv4();
  console.log(userId);
  res.render('index_consumer.ejs' , {userId});
})


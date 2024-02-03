// server.js
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const consumeMessages = require('./client');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const port = 9000;
  server.listen(port, () => {
    console.log(`Server is running`);
});

consumeMessages(wss);

app.get('/', (req, res) => {
  res.render('index_consumer.ejs');
})


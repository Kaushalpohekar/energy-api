const mqtt = require('mqtt');

// MQTT Broker Configuration
const broker = 'mqtt://broker.emqx.io';
const topic = 'demo/test/ota';

// MQTT Client
const client = mqtt.connect(broker);

// Subscribe to the MQTT topic
client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe(topic, (err) => {
    if (err) {
      console.error('Error subscribing to topic:', err);
    } else {
      console.log(`Subscribed to topic: ${topic}`);
    }
  });
});

// Handle incoming messages
client.on('message', (topic, message) => {
  let data;

  try {
    data = JSON.parse(message.toString());

    // Get the current date and time
    const timestamp = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });

    console.log(`Received message at ${timestamp}:`, data);
  } catch (error) {
    // Handle the case where the message is not valid JSON (dummy data)
    // console.log('Received message is not valid JSON:', message.toString());
  }
});

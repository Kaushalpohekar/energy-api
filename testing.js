const mqtt = require('mqtt');

// MQTT Broker Configuration
const broker = 'mqtt://broker.emqx.io';
const topic1 = 'MQTT/MANISH/1';
const topic2 = 'MQTT/MANISH/2';
const topic3 = 'MQTT/MANISH/3';
const topic4 = 'MQTT/MANISH/4';
const topic5 = 'MQTT/MANISH/5';
const topic6 = 'MQTT/MANISH/6';

// Store data from both topics
let dataFromTopic1 = null;
let dataFromTopic2 = null;
let dataFromTopic3 = null;
let dataFromTopic4 = null;
let dataFromTopic5 = null;
let dataFromTopic6 = null;

// MQTT Client
const client = mqtt.connect(broker);

// Subscribe to the MQTT topics
client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe([topic1, topic2, topic3, topic4, topic5, topic6], (err) => {
    if (err) {
      console.error('Error subscribing to topics:', err);
    } else {
      console.log(`Subscribed to topics: ${topic1}, ${topic2}, ${topic3}, ${topic4}, ${topic5}, ${topic6}`);
    }
  });
});

// Handle incoming messages
client.on('message', (topic, message) => {
  let data;

  try {
    data = JSON.parse(message.toString());

    // Store data based on the topic
    if (topic === topic1) {
      dataFromTopic1 = data;
    } else if (topic === topic2) {
      dataFromTopic2 = data;
    } else if (topic === topic3) {
      dataFromTopic3 = data;
    } else if (topic === topic4) {
      dataFromTopic4 = data;
    } else if (topic === topic5) {
      dataFromTopic5 = data;
    } else if (topic === topic6) {
      dataFromTopic6 = data;
    }

    // Check if both data sets are available and merge them into a single array
    if (dataFromTopic1 !== null && dataFromTopic2 !== null && dataFromTopic3 !== null && dataFromTopic4 !== null && dataFromTopic5 !== null && dataFromTopic6 !== null) {
      const mergedData = { ...dataFromTopic1, ...dataFromTopic2, ...dataFromTopic3, ...dataFromTopic4, ...dataFromTopic5, ...dataFromTopic6  };

      // Get the current date and time
      const timestamp = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });

      console.log(`Received messages for topics ${topic1}, ${topic2}, ${topic3}, ${topic4},${topic5} and ${topic6} at ${timestamp}:`);
      console.log('Merged Data:', mergedData);

      // Reset data for the next round
      dataFromTopic1 = null;
      dataFromTopic2 = null;
      dataFromTopic3 = null;
      dataFromTopic4 = null;
      dataFromTopic5 = null;
      dataFromTopic6 = null;
    }
  } catch (error) {
    // Handle the case where the message is not valid JSON (dummy data)
    // console.log('Received message is not valid JSON:', message.toString());
  }
});

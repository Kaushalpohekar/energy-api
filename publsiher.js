const mqtt = require('mqtt');

// MQTT broker URL
const brokerUrl = 'mqtt://your-mqtt-broker-url';

// Topics to publish data to
const topic1 = 'topic1';
const topic2 = 'topic2';
const topic3 = 'topic3';

// Create an MQTT client
const client = mqtt.connect(brokerUrl);

// Function to generate a random number within a specified range
function getRandomNumber(min, max) {
  const randomFloat = Math.random();
  const randomInRange = min + (randomFloat * (max - min));
  return Math.round(randomInRange);
}

// Function to publish random data to a topic
function publishRandomData(topic, deviceName) {
  const randomVoltage = getRandomNumber(200, 240);
  const randomCurrent = getRandomNumber(5, 10);

  const data = {
    device_uid: deviceName,
    voltage_1n: randomVoltage,
    current_1: randomCurrent,
    // Add other data fields as needed
  };

  client.publish(topic, JSON.stringify(data), () => {
    console.log(`Published data to ${topic}:`, data);
  });
}

// When connected to the MQTT broker, start publishing random data
client.on('connect', () => {
  console.log('Connected to MQTT broker');

  // Publish random data to topic1, topic2, and topic3 every 5 seconds
  setInterval(() => {
    publishRandomData(topic1, 'device1');
    publishRandomData(topic2, 'device2');
    publishRandomData(topic3, 'device3');
  }, 5000); // Change the interval as needed
});

// Handle errors
client.on('error', (error) => {
  console.error('MQTT Error:', error);
});

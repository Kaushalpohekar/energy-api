const mqtt = require('mqtt');

const options = {
    username: 'Sense2023',  // Replace with your MQTT username
    password: 'sense123'   // Replace with your MQTT password
};

const client = mqtt.connect('mqtt://dashboard.senselive.in:1883', options);

const topics = [
    'gauge/8d0608bd-09b8-467b-bbfb-52915e54dbbd/bbb3a9aa-f6b6-449d-b198-cb4a9a5126c5',
    'gauge/1a72e9c4-8d4c-4b6a-b124-a1f2b3c4d5e6/d13a09b0-5b4c-4d56-8e3a-1fa3c67183aa',
    'gauge/2b72d9a7-9c5d-4e6a-a234-b2e3f4c5d7e8/8c5bda9e-bf52-412e-a52e-96cb7b473c2f'
];

// function generateRandomData() {
//     return (Math.random() * (120 - 10) + 10).toFixed(2);
// }
function generateRandomData() {
  return (Math.random() * (12.50 - 10.50) + 10.50).toFixed(2);
}

function sendData() {
    const value = generateRandomData();
    topics.forEach((topic) => {
        console.log(`Publishing value: ${value} to topic: ${topic}`);
        client.publish(topic, JSON.stringify({ value: value }), { qos: 1 }, (err) => {
            if (err) {
                console.log(`Failed to publish to ${topic}:`, err);
            } else {
                console.log(`Data successfully published to ${topic}.`);
            }
        });
    });
}

client.on('connect', () => {
    console.log('Connected to MQTT broker');
    setInterval(sendData, 100);  // Publish data every 5 seconds
});

client.on('error', (err) => {
    console.log('MQTT connection error:', err);
});

client.on('close', () => {
    console.log('MQTT connection closed');
});
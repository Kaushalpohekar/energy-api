const mqtt = require('mqtt');

// MQTT broker URL
const brokerUrl = 'mqtt://broker.emqx.io';

// Topics to publish data to
const topic1 = 'Energy/SenseLive/SL02202350/1';
const topic2 = 'Energy/SenseLive/SL02202350/2';
const topic3 = 'Energy/SenseLive/SL02202350/3';

// Create an MQTT client
const client = mqtt.connect(brokerUrl);

// Function to generate a random number within a specified range
function getRandomNumber(min, max) {
  const randomFloat = Math.random();
  const randomInRange = min + randomFloat * (max - min);
  return Math.round(randomInRange);
}

// Function to publish random data to a topic
function publishRandomData1(topic, deviceName) {
  const voltage_1n = getRandomNumber(200, 240);
  const voltage_2n = getRandomNumber(5, 10);
  const voltage_3n = getRandomNumber(5, 10);
  const voltage_N = getRandomNumber(5, 10);
  const voltage_12 = getRandomNumber(5, 10);
  const voltage_23 = getRandomNumber(5, 10);
  const voltage_31 = getRandomNumber(5, 10);

  const data = {
    device_uid: deviceName,
    voltage_1n: voltage_1n,
    voltage_2n: voltage_2n,
    voltage_3n: voltage_3n,
    voltage_N: voltage_N,
    voltage_12: voltage_12,
    voltage_23: voltage_23,
    voltage_31: voltage_31,
  };

  client.publish(topic, JSON.stringify(data), () => {
    console.log(`Published data to ${topic}:`, data);
  });
}

function publishRandomData2(topic, deviceName) {
  const current_1 = getRandomNumber(5, 10);
  const current_2 = getRandomNumber(5, 10);
  const current_3 = getRandomNumber(5, 10);
  const kw_1 = getRandomNumber(5, 10);
  const kw_2 = getRandomNumber(5, 10);
  const kw_3 = getRandomNumber(5, 10);
  const pf_1 = getRandomNumber(5, 10);

  const data = {
    device_uid: deviceName,
    current_1: current_1,
    current_2: current_2,
    current_3: current_3,
    kw_1: kw_1,
    kw_2: kw_2,
    kw_3: kw_3,
    pf_1: pf_1,
  };

  client.publish(topic, JSON.stringify(data), () => {
    console.log(`Published data to ${topic}:`, data);
  });
}

function publishRandomData3(topic, deviceName) {
  const pf_2 = getRandomNumber(5, 10);
  const pf_3 = getRandomNumber(5, 10);
  const pf = getRandomNumber(5, 10);
  const freq = getRandomNumber(5, 10);
  const kw = getRandomNumber(5, 10);
  const kvar = getRandomNumber(5, 10);
  const kva = getRandomNumber(5, 10);

  const data = {
    device_uid: deviceName,
    pf_2: pf_2,
    pf_3: pf_3,
    pf: pf,
    freq: freq,
    kw: kw,
    kvar: kvar,
    kva: kva,
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
    publishRandomData1(topic1, 'SL02202350');
    publishRandomData2(topic2, 'SL02202350');
    publishRandomData3(topic3, 'SL02202350');
  }, 5000); // Change the interval as needed
});

// Handle errors
client.on('error', (error) => {
  console.error('MQTT Error:', error);
});

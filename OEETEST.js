const mqtt = require('mqtt');
const { Client } = require('pg');

const broker = 'ws://dashboard.senselive.in:9001';

const pgConfig2 = {
  host: 'data.senselive.in',
  user: 'senselive',
  password: 'SenseLive@2025',
  database: 'senselive_db',
  port: 5432,
  ssl: { rejectUnauthorized: false },
};

const pgClient2 = new Client(pgConfig2);

pgClient2.connect((err) => {
  if (err) {
    console.error('Error connecting to PostgreSQL database:', err.stack);
    return;
  }
  console.log('Connected to PostgreSQL database');
});

const options = {
  username: 'Sense2023',
  password: 'sense123',
};

const mqttClient = mqtt.connect(broker, options);

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');

  mqttClient.subscribe('machine/data/WIRESIMULATION', (error) => {
    if (error) {
      console.error('Error subscribing to topic:', error);
    } else {
      console.log('Subscribed to topic');
    }
  });
});

mqttClient.on('message', (topic, message) => {
  try {
    const messageStr = message.toString();
    const jsonData = JSON.parse(messageStr);

    const insertQuery = `INSERT INTO oee.device_data (DeviceUID, Timestamp, data) 
                         VALUES ($1, NOW(), $2)`;

    const insertValues = [
      jsonData.DeviceUID || 'OEETEST',
      JSON.stringify(jsonData)
    ];

    pgClient2.query(insertQuery, insertValues)
      .then(() => {
        console.log('Data inserted into PostgreSQL:', jsonData.DeviceUID);
      })
      .catch((error) => {
        console.error('PostgreSQL Insert Error:', error);
      });

  } catch (error) {
    console.error('Invalid JSON Format:', error);
  }

});

mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});

process.on('exit', () => {
  pgClient2.end();
});

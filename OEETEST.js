const mqtt = require('mqtt');
const { Client } = require('pg');

const broker = 'ws://dashboard.senselive.in:9001';

const pgConfig2 = {
  host: 'senselive.postgres.database.azure.com',
  user: 'kaushal',
  password: 'Kaushal@123',
  database: 'ems',
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
    const data = JSON.parse(message);

    console.log('Data Collected For Device', data.DeviceUID);

    const insertQuery = `INSERT INTO oee.device_data (DeviceUID, Timestamp, data) 
                         VALUES ($1, NOW(), $2)`;

    const insertValues = [
      data.DeviceUID || 'OEETEST',
      message,
    ];

    if (data) {
      pgClient2.query(insertQuery, insertValues)
        .then(() => {
          console.log('Data inserted into PostgreSQL for DeviceUID:');
        })
        .catch((error) => {
          console.error('Error inserting data into PostgreSQL', error);
        });
    } else {
      console.log('Data is missing DeviceUID, not inserted');
    }
  } catch (error) {
    console.error('Error processing message:', error);
  }
});

mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});

process.on('exit', () => {
  pgClient2.end();
});

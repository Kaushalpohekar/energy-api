const mqtt = require('mqtt');
const { Client } = require('pg');

// MQTT Broker details
const broker = 'ws://dashboard.senselive.in:9001';

// PostgreSQL connection configuration
const pgConfig2 = {
  host: 'pgsql.senselive.in',
  user: 'senselive',
  password: 'SenseLive',
  database: 'ems',
  port: 5432,
};

// Connect to PostgreSQL
const pgClient2 = new Client(pgConfig2);

pgClient2.connect((err) => {
  if (err) {
    console.error('Error connecting to PostgreSQL database:', err.stack);
    return;
  }
  console.log('Connected to PostgreSQL database');
});

// MQTT connection options
const options = {
  username: 'Sense2023',
  password: 'sense123',
};

// Connect to the MQTT broker
const mqttClient = mqtt.connect(broker, options);

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  
  // Subscribe to the bansal_wire/plc topic
  mqttClient.subscribe('bansal_wire/#', (error) => {
    if (error) {
      console.error('Error subscribing to bansal_wire/plc topic:', error);
    } else {
      console.log('Subscribed to bansal_wire/PLC topic');
    }
  });
});

mqttClient.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message);

    // Log the DeviceUID
    console.log('Data Collected For Device', data.DeviceUID);

    // Prepare the INSERT query
    const insertQuery = `INSERT INTO ems_schema.device_data (DeviceUID, Timestamp, data) 
                         VALUES ($1, NOW(), $2)`;

    const insertValues = [
      data.DeviceUID || 'SLBANSALPLC',
      message, // Insert the entire JSON message
    ];

    // Check if DeviceUID is present
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

// Clean up resources when the process exits
process.on('exit', () => {
  pgClient2.end();
});

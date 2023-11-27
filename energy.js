const mqtt = require('mqtt');
const { Pool } = require('pg'); // Use the 'pg' package for PostgreSQL

//const SL02202326 = require('./SL02202326');
//const SL02202328 = require('./SL02202328');
//const SL02202329 = require('./SL02202329');
//const SL02202344 = require('./SL02202344');
//const SL02202345 = require('./SL02202345');
//const SL02202346 = require('./SL02202346');
const SL02202347 = require('./SL02202347');
//const SL02202348 = require('./SL02202348');

const broker = 'mqtt://broker.emqx.io';


// MQTT Broker Configuration

const topic = 'MQTT/kapilansh';

// PostgreSQL Database Configuration
const dbConfig = {
  host: '64.227.181.131',
  user: 'postgres',
  password: 'iotsenselive',
  database: 'senselive_db',
  port: 12440, // Change the port to your PostgreSQL port (5432 is the default)
};

// MQTT Client
const client = mqtt.connect(broker);

// Create a PostgreSQL connection pool
const pool = new Pool(dbConfig);

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


client.on('message', (topic, message) => {
  let data;

  try {
    data = JSON.parse(message.toString());

    // Check if the received message has the correct API key
    if (data.api_key === '2024') {
      // API key matches, insert the data into the PostgreSQL database
      insertDataIntoDatabase(data, (error) => {
        if (error) {
          console.error('Error inserting data into the database:', error);
        } else {
          console.log('Data inserted into the database.');
        }
      });
    } else {
      console.log('Received message with invalid API key:', data.api_key);
    }
  } catch (error) {
    // Handle the case where the message is not valid JSON (dummy data)
    const timeOfDummyData = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
    //console.log('Received message is not valid JSON:', message.toString(), timeOfDummyData);
  }
});

// Function to insert data into the PostgreSQL database with a callback
function insertDataIntoDatabase(data, callback) {
  // Fetch the latest entry ID from the database
  pool.query(
    'SELECT MAX(id) FROM energy_database',
    (error, results) => {
      if (error) {
        callback(error);
      } else {
        const latestId = results.rows[0].max || 0; // Get the latest ID or default to 0 if no records exist

        // Increment the latest ID by one to use it as the new insertion ID
        const newId = latestId + 1;

        const values = [
          newId, // Use the new ID as the insertion ID
          new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }),
          data.device,
          data.voltage_1n,
          data.voltage_2n,
          data.voltage_3n,
        ];

        // If any of the values is undefined, replace it with 0
        for (let i = 2; i < values.length - 1; i++) {
          if (values[i] === undefined) {
            values[i] = 0;
          }
        }

        pool.query(
          'INSERT INTO energy_database (id, date_time, device_uid, voltage_1n, voltage_2n, voltage_3n) VALUES ($1, $2, $3, $4, $5, $6)',
          values, // Make sure all six values are provided
          (error, results) => {
            if (error) {
              callback(error);
            } else {
              callback(null);
            }
          }
        );
      }
    }
  );
}

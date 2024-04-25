const mqtt = require('mqtt');
const { Pool } = require('pg');

const broker = 'mqtt://dashboard.senselive.in:1883';

const options = {
  username: 'Sense2023',
  password: 'sense123',
  clientId: 'kapilansh-mqtt'
};

const topic = 'MQTT/kapilansh';
const dbConfig = {
  host: '3.110.101.216',
  user: 'postgres',
  password: 'sense123',
  database: 'ems',
  port: 5432,
};

const client = mqtt.connect(broker,options);

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
      // API key matches, insert the data into the PostgreSQL database
      insertDataIntoDatabase(data, (error) => {
        if (error) {
          console.error('Error inserting data into the database:', error);
        } else {
          console.log(data);
          console.log('Data inserted into the database.');
        }
      });
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
    'SELECT MAX(id) FROM ems_schema.ems_actual_data',
    (error, results) => {
      if (error) {
        callback(error);
      } else {
        const latestId = results.rows[0].max || 0; // Get the latest ID or default to 0 if no records exist

        // Increment the latest ID by one to use it as the new insertion ID
        const newId = latestId + 1;

        const values = [
          newId, // Use the new ID as the insertion ID
          new Date(),
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
          'INSERT INTO ems_schema.ems_actual_data (id, date_time, device_uid, voltage_1n, voltage_2n, voltage_3n) VALUES ($1, $2, $3, $4, $5, $6)',
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

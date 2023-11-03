const mqtt = require('mqtt');
const mysql = require('mysql2');

// MQTT Broker Configuration
const broker = 'mqtt://broker.emqx.io';
const topic1 = 'Energy/SenseLive/SL02202345/1';
const topic2 = 'Energy/SenseLive/SL02202345/2';

// Store data from both topics
let dataFromTopic1 = null;
let dataFromTopic2 = null;

// MQTT Client
const client = mqtt.connect(broker);

// Subscribe to the MQTT topics
client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe([topic1, topic2], (err) => {
    if (err) {
      console.error('Error subscribing to topics:', err);
    } else {
      console.log(`Subscribed to topics: ${topic1}, ${topic2}`);
    }
  });
});

// MySQL Database Configuration
const dbConfig = {
  host: 'senselivedb.cn5vfllmzwrp.ap-south-1.rds.amazonaws.com', // Replace with your MySQL host
  user: 'admin', // Replace with your MySQL username
  database: 'senselive_db', // Replace with your MySQL database name
  password: 'sense!123', // Replace with your MySQL password
  port: 3306, // Replace with your MySQL port
};

// Create a MySQL connection
const dbConnection = mysql.createConnection(dbConfig);

// Connect to the MySQL database
dbConnection.connect((err) => {
  if (err) {
    console.error('Error connecting to MySQL database:', err);
  } else {
    console.log('Connected to MySQL database');
  }
});

client.on('message', async (topic, message) => {
  let data;

  try {
    data = JSON.parse(message.toString());

    // Store data based on the topic
    if (topic === topic1) {
      dataFromTopic1 = data;
    } else if (topic === topic2) {
      dataFromTopic2 = data;
    }

    // Check if all data sets are available and merge them into a single object
    if (dataFromTopic1 && dataFromTopic2) {
      const device_uid = data.device_uid;
      const mergedData = {
        device_uid, // Replace with your actual device UID
        date_time: new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }),
        ...dataFromTopic1,
        ...dataFromTopic2
      };

      // Insert the merged data into the MySQL database
      try {
        const {
          device_uid, date_time, orp, pump_1, pump_2 } = mergedData;
        const query = `INSERT INTO ORP_Meter (device_uid, date_time, orp, pump_1, pump_2) VALUES (?, ?, ?, ?, ?)`;
        const values = [device_uid, date_time, orp, pump_1, pump_2];
        
        dbConnection.query(query, values, (err, results) => {
          if (err) {
            console.error('Error inserting data into MySQL:', err);
          } else {
            console.log('Data inserted into MySQL database.');
          }
        });
      } catch (err) {
        console.error('Error inserting data into MySQL:', err);
      }

      // Reset data for the next round
      dataFromTopic1 = null;
      dataFromTopic2 = null;
    }
  } catch (error) {
    // Handle the case where the message is not valid JSON (dummy data)
    // console.log('Received message is not valid JSON:', message.toString());
  }
});

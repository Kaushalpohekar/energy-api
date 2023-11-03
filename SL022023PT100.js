const mqtt = require('mqtt');
const mysql = require('mysql');

// MQTT Broker Configuration
const broker = 'mqtt://broker.emqx.io';
const topic1 = 'TempPT100/SenseLive/SL022023PT100/1';

// Store data from both topics
let dataFromTopic1 = null;

// MQTT Client
const client = mqtt.connect(broker);

// MySQL Database Configuration
const dbConfig = {
  host: 'senselivedb.cn5vfllmzwrp.ap-south-1.rds.amazonaws.com',
  user: 'admin',
  password: 'sense!123',
  database: 'senselive_db',
  port: 3306, // Change to your MySQL port (default is 3306)
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

// Handle incoming messages
client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe(topic1, (err) => {
    if (err) {
      console.error('Error subscribing to topic:', err);
    } else {
      console.log(`Subscribed to topic: ${topic1}`);
    }
  });
});

client.on('message', handleIncomingMessage);

function handleIncomingMessage(topic, message) {
  try {
    const data = JSON.parse(message.toString());

    if (topic === topic1) {
      dataFromTopic1 = data;
    }

    if (dataFromTopic1) {
      const mergedData = {
        device_uid: "SL022023PT100",
        ...dataFromTopic1,
      };

      insertDataIntoDatabase(mergedData);
      dataFromTopic1 = null;
    }
  } catch (error) {
    console.error('Received message is not valid JSON:', message.toString());
  }
}

function insertDataIntoDatabase(mergedData) {
  const { device_uid, temperature } = mergedData;
  // Round the temperature to two decimal places
  const roundedTemperature = parseFloat(temperature).toFixed(2);
  const query = `INSERT INTO elkem_temp (device_uid, temp) VALUES (?, ?)`;

  dbConnection.query(query, [device_uid, roundedTemperature], (error, results) => {
    if (error) {
      console.error('Error inserting data into MySQL:', error);
    } else {
      console.log(mergedData);
      console.log('Data inserted into MySQL database.');
    }
  });
}
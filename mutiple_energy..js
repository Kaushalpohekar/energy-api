const mqtt = require('mqtt');
const { Client } = require('pg'); // Import the PostgreSQL Client

// MQTT Broker Configuration
const broker = 'mqtt://broker.emqx.io';

// Define the base topic
const baseTopic = 'mqtt/';

// Define an array to store the topic numbers
const topicNumbers = ['1', '2', '3', '4', '5', '6'];

// Create an array to store all the topics to subscribe to
const topicsToSubscribe = topicNumbers.map((number) => `${baseTopic}${number}`);

// Store data for each topic as it becomes available
const dataByTopic = {};

// MQTT Client
const client = mqtt.connect(broker);

// Subscribe to all MQTT topics
client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe('#', (err) => {
    if (err) {
      console.error('Error subscribing to topics:', err);
    } else {
      console.log('Subscribed to all topics');
    }
  });
});

// PostgreSQL Database Configuration
const dbConfig = {
  user: 'postgres',
  host: '64.227.181.131',
  database: 'senselive_db',
  password: 'iotsenselive',
  port: 12440, // Change to your PostgreSQL port
};

// Create a PostgreSQL client
const dbClient = new Client(dbConfig);

// Connect to the PostgreSQL database
dbClient.connect()
  .then(() => {
    console.log('Connected to PostgreSQL database');
  })
  .catch((err) => {
    console.error('Error connecting to PostgreSQL database:', err);
  });

// Function to fetch the latest ID and increment it by one
async function getNextId() {
  try {
    const query = 'SELECT id FROM energy_database ORDER BY id DESC LIMIT 1';
    const result = await dbClient.query(query);

    if (result.rows.length === 0) {
      return 1; // If no rows exist, start with ID 1
    } else {
      const latestId = result.rows[0].id;
      return latestId + 1;
    }
  } catch (err) {
    console.error('Error fetching the latest ID:', err);
    throw err;
  }
}

// Handle incoming messages
client.on('message', async (topic, message) => {
  let data;

  try {
    data = JSON.parse(message.toString());

    // Check if the received topic starts with the baseTopic and matches the expected format (mqtt/{number})
    if (topic.startsWith(baseTopic)) {
      const topicNumber = topic.replace(baseTopic, ''); // Extract the topic number

      // Store data for the corresponding topic number
      dataByTopic[topicNumber] = data;

      // Check if data is available for all desired topics
      const allTopicsDataAvailable = topicNumbers.every((number) => dataByTopic[number]);

      if (allTopicsDataAvailable) {
        // You have data for all desired topics, you can process and insert it into the database here
        try {
          const nextId = await getNextId(); // Get the next ID
          const {
            // ... (your data fields here)
          } = dataByTopic; // Replace with your data field names

          const query = `INSERT INTO energy_database (id, device_uid, date_time, ... /* Add all your data fields */) 
                         VALUES ($1, $2, $3, ... /* Add placeholders for all your data fields */)`;
          await dbClient.query(query, [nextId, /* Provide values for all your data fields */]);
          console.log('Data inserted into PostgreSQL database.');
        } catch (err) {
          console.error('Error inserting data into PostgreSQL:', err);
        }

        // Clear the data for these topics
        topicNumbers.forEach((number) => {
          delete dataByTopic[number];
        });
      }
    }
  } catch (error) {
    // Handle the case where the message is not valid JSON (dummy data)
    // console.log('Received message is not valid JSON:', message.toString());
  }
});

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
  connectionTimeoutMillis: 30000,
  idle_in_transaction_session_timeout: 60000,
  statement_timeout: 60000
};

let pgClient2;

function connectToDatabase() {
  pgClient2 = new Client(pgConfig2);

  pgClient2.connect()
    .then(() => {
      console.log('Connected to PostgreSQL database');
      keepConnectionAlive();
    })
    .catch((err) => {
      console.error('Database connection error:', err);
      setTimeout(connectToDatabase, 5000);
    });

  pgClient2.on('error', (err) => {
    console.error('PostgreSQL Client Error:', err);
    if (err.code === 'ETIMEDOUT' || err.code === 'ECONNRESET') {
      connectToDatabase();
    }
  });
}

function keepConnectionAlive() {
  setInterval(() => {
    pgClient2.query('SELECT 1')
      .then(() => console.log('PostgreSQL Connection Alive'))
      .catch(err => {
        console.error('Keep-Alive Query Failed:', err);
        connectToDatabase();
      });
  }, 60000);
}

connectToDatabase();

const options = {
  username: 'Sense2023',
  password: 'sense123',
};

const mqttClient = mqtt.connect(broker, options);

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');

  mqttClient.subscribe('machine/data/WIRESIMULATION', (error) => {
    if (error) console.error('Error subscribing to topic:', error);
  });
});

mqttClient.on('message', async (topic, message) => {
  try {
    if (!message || message.length === 0) return;

    const messageStr = message.toString().trim();
    let jsonData;

    try {
      jsonData = JSON.parse(messageStr);
    } catch {
      return;
    }

    if (!jsonData || typeof jsonData !== 'object') return;

    const insertQuery = `INSERT INTO oee.device_data (DeviceUID, Timestamp, data) VALUES ($1, NOW(), $2)`;
    const insertValues = [jsonData.DeviceUID || 'OEETEST', JSON.stringify(jsonData)];

    try {
      await pgClient2.query(insertQuery, insertValues);
      console.log(`Data inserted: ${jsonData.DeviceUID}`);
    } catch (dbError) {
      console.error('PostgreSQL Insert Error:', dbError.message);
    }
  } catch (error) {
    console.error('Error Processing MQTT Message:', error.message);
  }
});

mqttClient.on('error', (error) => console.error('MQTT error:', error));

mqttClient.on('close', () => console.warn('MQTT connection closed. Reconnecting...'));

process.on('SIGINT', async () => {
  await pgClient2.end();
  mqttClient.end();
  process.exit();
});

const mqtt = require('mqtt');
const { Client } = require('pg');

// Configuration Details
const config = {
  mqttBroker: 'mqtt://broker.emqx.io',
  mqttTopics: [
    'Energy/SenseLive/SL02202346/1',
    'Energy/SenseLive/SL02202346/2',
    'Energy/SenseLive/SL02202346/3',
  ],
  dbConfig: {
    user: 'postgres',
    host: '64.227.181.131',
    database: 'senselive_db',
    password: 'iotsenselive',
    port: 12440,
  },
  dbColumns: [
    'voltage_1n', 'voltage_2n', 'voltage_3n', 'voltage_N', 'voltage_12',
    'voltage_23', 'voltage_31', 'current_1', 'current_2', 'current_3',
    'kw_1', 'kw_2', 'kw_3', 'pf_1', 'pf_2', 'pf_3', 'pf', 'freq', 'kw', 'kvar', 'kva'
  ],
};

const mqttClient = mqtt.connect(config.mqttBroker);
const dbClient = new Client(config.dbConfig);

const dataFromTopics = {};

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  mqttClient.subscribe(config.mqttTopics, (err) => {
    if (err) {
      console.error('Error subscribing to topics:', err);
    } else {
      console.log('Subscribed to topics:', config.mqttTopics.join(', '));
    }
  });
});

dbClient.connect()
  .then(() => {
    console.log('Connected to PostgreSQL database');
  })
  .catch((err) => {
    console.error('Error connecting to PostgreSQL database:', err);
  });

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

mqttClient.on('message', async (topic, message) => {
  try {
    const data = JSON.parse(message.toString());
    dataFromTopics[topic] = data;

    if (Object.keys(dataFromTopics).length === config.mqttTopics.length) {
      const mergedData = { device_uid: data.device_uid, date_time: new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }) };
      config.mqttTopics.forEach((topic) => {
        Object.assign(mergedData, dataFromTopics[topic]);
      });

      try {
        const nextId = await getNextId();
        const query = `INSERT INTO energy_database (id, device_uid, date_time, ${config.dbColumns.join(', ')}) VALUES ($1, $2, $3, ${config.dbColumns.map((col, idx) => `$${idx + 4}`).join(', ')})`;
        await dbClient.query(query, [nextId, mergedData.device_uid, mergedData.date_time, ...config.dbColumns.map(col => mergedData[col])]);
        console.log('Data inserted into PostgreSQL database.', mergedData.date_time);
      } catch (err) {
        console.error('Error inserting data into PostgreSQL:', err);
      }

      Object.keys(dataFromTopics).forEach((topic) => {
        dataFromTopics[topic] = null;
      });
    }
  } catch (error) {
    console.error('Error processing MQTT message:', error);
  }
});

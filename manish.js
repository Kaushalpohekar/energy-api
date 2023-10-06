const mqtt = require('mqtt');
const { Client } = require('pg'); // Import the PostgreSQL Client

// MQTT Broker Configuration
const broker = 'mqtt://broker.emqx.io';
const topic1 = 'Energy/SenseLive/SL02202346/1';
const topic2 = 'Energy/SenseLive/SL02202346/2';
const topic3 = 'Energy/SenseLive/SL02202346/3';
const topic4 = 'Energy/SenseLive/SL02202346/4';
const topic5 = 'Energy/SenseLive/SL02202346/5';
const topic6 = 'Energy/SenseLive/SL02202346/6';

// Store data from both topics
let dataFromTopic1 = null;
let dataFromTopic2 = null;
let dataFromTopic3 = null;
let dataFromTopic4 = null;
let dataFromTopic5 = null;
let dataFromTopic6 = null;

// MQTT Client
const client = mqtt.connect(broker);

// Subscribe to the MQTT topics
client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe([topic1, topic2, topic3, topic4, topic5, topic6], (err) => {
    if (err) {
      console.error('Error subscribing to topics:', err);
    } else {
      console.log(`Subscribed to topics: ${topic1}, ${topic2}, ${topic3}, ${topic4}, ${topic5}, ${topic6}`);
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

    // Store data based on the topic
    if (topic === topic1) {
      dataFromTopic1 = data;
    } else if (topic === topic2) {
      dataFromTopic2 = data;
    } else if (topic === topic3) {
      dataFromTopic3 = data;
    } else if (topic === topic4) {
      dataFromTopic4 = data;
    } else if (topic === topic5) {
      dataFromTopic5 = data;
    } else if (topic === topic6) {
      dataFromTopic6 = data;
    }

    // Check if all data sets are available and merge them into a single object
    if (dataFromTopic1 && dataFromTopic2 && dataFromTopic3 && dataFromTopic4 && dataFromTopic5 && dataFromTopic6) {
      const device_uid = data.device_uid;
      const mergedData = {
        device_uid, // Replace with your actual device UID
        date_time: new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }),
        ...dataFromTopic1,
        ...dataFromTopic2,
        ...dataFromTopic3,
        ...dataFromTopic4,
        ...dataFromTopic5,
        ...dataFromTopic6,
      };

      // Insert the merged data into the PostgreSQL database
      try {
        const nextId = await getNextId(); // Get the next ID
        const {
          device_uid, date_time, voltage_1n, voltage_2n, voltage_3n, voltage_N, voltage_12,
          voltage_23, voltage_31, voltage_L, current_1, current_2, current_3, current, kw_1, kw_2,
          kw_3, kvar_1, kvar_2, kvar_3, kva_1, kva_2, kva_3, pf_1, pf_2, pf_3, pf, freq, kw, kvar,
          kva, max_kw,  min_kw, max_kvar, min_kvar,  max_kva, max_int_v1n, max_int_v2n, max_int_v3n,
          max_int_v12, max_int_v23, max_int_v31, max_int_i1, max_int_i2, max_int_i3,  imp_kwh, exp_kwh,
          kwh, imp_kvarh, exp_kvarh, kvarh, kvah, run_h, on_h, thd_v1n, thd_v2n, thd_v3n, thd_v12, thd_v23,
          thd_v31, thd_i1, thd_i2, thd_i3 } = mergedData;
        const query = `INSERT INTO energy_database (id, device_uid, date_time, voltage_1n, voltage_2n, voltage_3n,
                          voltage_N, voltage_12, voltage_23, voltage_31, voltage_L, current_1, current_2, current_3,
                          current, kw_1, kw_2, kw_3, kvar_1, kvar_2, kvar_3, kva_1, kva_2, kva_3, pf_1, pf_2, pf_3,
                          pf, freq, kw, kvar, kva, max_kw, min_kw, max_kvar, min_kvar, max_kva, max_int_v1n, max_int_v2n,
                          max_int_v3n, max_int_v12, max_int_v23, max_int_v31, max_int_i1, max_int_i2, max_int_i3, imp_kwh,
                          exp_kwh, kwh, imp_kvarh, exp_kvarh, kvarh, kvah, run_h, on_h, thd_v1n, thd_v2n, thd_v3n, thd_v12,
                          thd_v23, thd_v31, thd_i1, thd_i2, thd_i3) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
                          $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, 
                          $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, 
                          $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64)`;
        await dbClient.query(query, [nextId, device_uid, date_time, voltage_1n, voltage_2n, voltage_3n, voltage_N, voltage_12,
          voltage_23, voltage_31, voltage_L, current_1, current_2, current_3, current, kw_1, kw_2,
          kw_3, kvar_1, kvar_2, kvar_3, kva_1, kva_2, kva_3, pf_1, pf_2, pf_3, pf, freq, kw, kvar,
          kva, max_kw,  min_kw, max_kvar, min_kvar,  max_kva, max_int_v1n, max_int_v2n, max_int_v3n,
          max_int_v12, max_int_v23, max_int_v31, max_int_i1, max_int_i2, max_int_i3,  imp_kwh, exp_kwh,
          kwh, imp_kvarh, exp_kvarh, kvarh, kvah, run_h, on_h, thd_v1n, thd_v2n, thd_v3n, thd_v12, thd_v23,
          thd_v31, thd_i1, thd_i2, thd_i3]);
        console.log('Data inserted into PostgreSQL database.');
      } catch (err) {
        console.error('Error inserting data into PostgreSQL:', err);
      }

      // Reset data for the next round
      dataFromTopic1 = null;
      dataFromTopic2 = null;
      dataFromTopic3 = null;
      dataFromTopic4 = null;
      dataFromTopic5 = null;
      dataFromTopic6 = null;
    }
  } catch (error) {
    // Handle the case where the message is not valid JSON (dummy data)
    // console.log('Received message is not valid JSON:', message.toString());
  }
});


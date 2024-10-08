// const mqtt = require('mqtt');
// const { Client } = require('pg');

// const broker = 'ws://dashboard.senselive.in:9001';

// // const pgConfig2 = {
// //   host: 'pgsql.senselive.in',
// //   user: 'senselive',
// //   password: 'SenseLive',
// //   database: 'ems',
// //   port: 5432,
// // };
// const pgConfig2 = {
//   host: 'pgsql.senselive.in',
//   user: 'senselive',
//   password: 'SenseLive',
//   database: 'ems',
//   port: 5432,
// };

// const pgClient2 = new Client(pgConfig2);

// pgClient2.connect((err) => {
//   if (err) {
//     console.error('Error connecting to PostgreSQL database:', err.stack);
//     return;
//   }
//   console.log('Connected to PostgreSQL database');
// });

// const options = {
//   username: 'Sense2023',
//   password: 'sense123',
// };

// const mqttClient = mqtt.connect(broker, options);

// mqttClient.on('connect', () => {
//   console.log('Connected to MQTT broker');
//   mqttClient.subscribe('TN-08:D1:F9:A7:56:B4/data/#', (error) => {
//     if (error) {
//       console.error('Error subscribing to topics:', error);
//     } else {
//       console.log('Subscribed to topics');
//     }
//   });
// });

// mqttClient.on('message', (topic, message) => {
//   try {
//     const data = JSON.parse(message);

//     console.log('Data Collected For Device', data.ID);

//     const insertQuery = `INSERT INTO ems_schema.Testing (DeviceUID, Timestamp, Width) VALUES ($1, $2, $3)`;

//     const insertValues = [
//       data.ID,                      // DeviceUID
//       new Date(data.TS).toISOString(), // Timestamp
//       data.data.W                   // Width
//     ];

//     if (data.ID) {
//       pgClient2.query(insertQuery, insertValues)
//         .then(() => {
//           console.log('Data inserted into ad table.');
//         })
//         .catch((error) => {
//           console.error('Error inserting data into ad table', error);
//         });
//     } else {
//       console.log('Data is not inserted due to missing DeviceUID');
//     }
//   } catch (error) {
//     console.error('Error processing message:', error);
//   }
// });

// mqttClient.on('error', (error) => {
//   console.error('MQTT error:', error);
// });
const mqtt = require('mqtt');
const { Client } = require('pg');

const broker = 'ws://dashboard.senselive.in:9001';

const pgConfig2 = {
  host: 'pgsql.senselive.in',
  user: 'senselive',
  password: 'SenseLive',
  database: 'ems',
  port: 5432,
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
  
  mqttClient.subscribe('Energy/Sense/Live/SL02202426-1', (error) => {
    if (error) {
      console.error('Error subscribing to Energy/Sense/Live/SL02202426-1 topic:', error);
    } else {
      console.log('Subscribed to bansal_wire/PLC topic');
    }
  });
});

mqttClient.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message);

    console.log('Data Collected For Device', data.DeviceUID);

    const insertQuery = `INSERT INTO ems_schema.device_data (DeviceUID, Timestamp, data) 
                         VALUES ($1, NOW(), $2)`;

    const insertValues = [
      data.DeviceUID || 'SLBANSALPLC',
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

const mqtt = require('mqtt');
const { Client } = require('pg');

const broker = 'ws://dashboard.senselive.in:9001';

const pgConfig2 = {
  host: '3.110.101.216',
  user: 'postgres',
  password: 'sense123',
  database: 'ems',
  port: 5432,
};

const pgClient2 = new Client(pgConfig2);

pgClient2.connect((err) => {
  if (err) {
    console.error('Error connecting to OUR database:', err.stack);
    return;
  }
  console.log('Connected to OUR PostgreSQL database');
});

const options = {
  username: 'Sense2023',
  password: 'sense123',
};

const mqttClient = mqtt.connect(broker, options);

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  mqttClient.subscribe('Energy/#', (error) => {
    if (error) {
      console.error('Error subscribing to all topics:', error);
    } else {
      console.log('Subscribed to all topics');
    }
  });
});

mqttClient.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message);

    console.log('Data Collected For Device', data.device_uid || data.DeviceUID);

    const insertQueryInEMS = `INSERT INTO ems_schema.ems_actual_data (
      device_uid, voltage_1n, voltage_2n, voltage_3n, voltage_n, voltage_12, voltage_23, voltage_31, voltage_l, 
      current_1, current_2, current_3, current, kw_1, kw_2, kw_3, kvar_1, kvar_2, kvar_3, kva_1, kva_2, kva_3, pf_1, pf_2, pf_3, 
      pf, freq, kw, kvar, kva, imp_kwh, exp_kwh, kwh, imp_kvarh, exp_kvarh, kvarh, kvah, thd_v1n, thd_v2n, thd_v3n, thd_v12, 
      thd_v23, thd_v31, thd_i1, thd_i2, thd_i3, max_kw, min_kw, max_kvar, min_kvar, max_int_v1n, max_int_v2n, max_int_v3n, 
      max_int_v12, max_int_v23, max_int_v31, max_kva, max_int_i1, max_int_i2, max_int_i3, run_h, on_h,ser_no,date_time
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, 
      $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, 
      $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, NOW()
    )`;

    const insertValuesInEMS = [ 
          data.DeviceUID, 
          data.V_1n,
          data.V_2n,
          data.V_3n,
          data.V_N,
          data.V_12,
          data.V_23,
          data.V_31,
          data.V_L,
          data.I_1,
          data.I_2,
          data.I_3,
          data.I,
          data.kw_1,
          data.kw_2,
          data.kw_3,
          data.kvar_1,
          data.kvar_2,
          data.kvar_3,
          data.kva_1,
          data.kva_2,
          data.kva_3,
          data.pf_1,
          data.pf_2,
          data.pf_3,
          data.pf,
          data.freq,
          data.kw / 1000 || null,
          data.kvar,
          data.kva,
          data.imp_kwh,
          data.exp_kwh,
          data.kwh / 1000 || null,
          data.imp_kvarh,
          data.exp_kvarh,
          data.kvarh,
          data.kvah,
          data.thd_v1n,
          data.thd_v2n,
          data.thd_v3n,
          data.thd_v12,
          data.thd_v23,
          data.thd_v31,
          data.thd_i1,
          data.thd_i2,
          data.thd_i3,
          data.max_kw,
          data.min_kw,
          data.max_kvar,
          data.min_kvar,
          data.max_int_v1n,
          data.max_int_v2n,
          data.max_int_v3n,
          data.max_int_v12,
          data.max_int_v23,
          data.max_int_v31,
          data.max_kva,
          data.max_int_i1,
          data.max_int_i2,
          data.max_int_i3,
          data.run_h,
          data.on_h,
          data.ser_no ]

    if(data.DeviceUID){
      pgClient2.query(insertQueryInEMS,insertValuesInEMS)
        .then(() => {
          console.log('Data inserted in OUR db.');
          receivedData = {};
        })
        .catch((error) => {
          console.error('Error inserting data into OUR db',error);
      });
    } else {
      console.log('Data is not Inserted For the DeviceUID:- ', data.device_uid || data.DeviceUID);
    }   
  } catch (error) {
    console.error('Error processing message:', error);
  }
});

mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});

// process.on('exit', () => {
//   mysqlPool.end();
// });

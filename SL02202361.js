const mqtt = require('mqtt');
const { Client } = require('pg');
const broker = 'ws://dashboard.senselive.in:9001';
const options = {
    username: 'Sense2023',
    password: 'sense123', 
  };
 
// db configuration 
const pgConfig = {
    host: '64.227.181.131',
    user: 'postgres',
    password: 'iotsenselive',
    database: 'senselive_db',
    port: 12440,
};
const pgClient = new Client(pgConfig);
pgClient.connect((err) => {
    if (err) {
      console.error('Error connecting to database:', err.stack);
      return;
    }
    console.log('Connected to PostgreSQL database');
});
                                                                      
                                                                       
// broker connection
const mqttClient = mqtt.connect(broker,options);

// required parameters
const requiredParameters =['device_uid', 'V_1n', 'V_2n', 'V_3n', 'V_N', 'V_12','V_23', 'V_31', 'V_L', 'pf',
 'freq', 'exp_kwh', 'kwh','imp_kvarh', 'exp_kvarh', 'kvarh', 'kvah', 'run_h', 'on_h', 'I_1', 
 'I_2', 'I_3', 'I', 'kw_1', 'kw_2','kw_3', 'kvar_1', 'kvar_2', 'kvar_3', 'kva_1',
 'kva_2', 'kva_3','pf_1', 'pf_2', 'pf_3','kw', 'kvar', 'ser_no','kva','max_kw',
  'min_kw', 'max_kvar', 'min_kvar',  'max_kva','max_int_v1n', 'max_int_v2n', 'max_int_v3n','max_int_v12', 'max_int_v23', 'max_int_v31',
 'max_int_i1', 'max_int_i2', 'max_int_i3',  'imp_kwh',  'thd_v1n', 'thd_v2n', 'thd_v3n', 'thd_v12', 'thd_v23','thd_v31',
 'thd_i1', 'thd_i2','thd_i3'];

//  insert query
 const insertQuery = `INSERT INTO public.energy_database (
  id, date_time, device_uid, voltage_1n, voltage_2n, voltage_3n, voltage_n, voltage_12, voltage_23, voltage_31, voltage_l, 
  current_1, current_2, current_3, current, kw_1, kw_2, kw_3, kvar_1, kvar_2, kvar_3, kva_1, kva_2, kva_3, pf_1, pf_2, pf_3, 
  pf, freq, kw, kvar, kva, imp_kwh, exp_kwh, kwh, imp_kvarh, exp_kvarh, kvarh, kvah, thd_v1n, thd_v2n, thd_v3n, thd_v12, 
  thd_v23, thd_v31, thd_i1, thd_i2, thd_i3, max_kw, min_kw, max_kvar, min_kvar, max_int_v1n, max_int_v2n, max_int_v3n, 
  max_int_v12, max_int_v23, max_int_v31, max_kva, max_int_i1, max_int_i2, max_int_i3, run_h, on_h,ser_no
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, 
  $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, 
  $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65
)`;

// declaring all 16 topics
const topics = Array.from({ length: 16 }, (_, i) => `Energy/Sense/Live/SL02202361/${i + 1}`);

// connecting to mqtt and subscribing to topics
mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');

topics.forEach((topic) => {
    mqttClient.subscribe(topic, (error) => {
      if (error) {
        console.error(`Error subscribing to ${topic}:`, error);
      } else {
        console.log(`Subscribed to ${topic}`);
      }
    });
  });
});

let receivedData = {};

mqttClient.on('message', (topic, message) => {
    const data = JSON.parse(message.toString());
    const dataKeys = Object.keys(data);
    
    dataKeys.forEach(key => {
        receivedData[key] = data[key];
    });

    const allParametersPresent = requiredParameters.every(param => receivedData.hasOwnProperty(param));
    
    if (allParametersPresent) {
        console.log("All required parameters are present!");
        // console.log(receivedData);
        // const nextId = getNextId();
        const getMaxIdQuery = 'SELECT MAX(id) FROM public.energy_database';

        pgClient.query(getMaxIdQuery)
          .then((result) => {
          const maxId = result.rows[0].max || 0;
          // Increment the id for the new entry
          const newId = maxId + 1;

        date_time = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
        const insertValues = [newId, 
          date_time, 
          receivedData.device_uid, 
          receivedData.V_1n,
          receivedData.V_2n,
          receivedData.V_3n,
          receivedData.V_N,
          receivedData.V_12,
          receivedData.V_23,
          receivedData.V_31,
          receivedData.V_L,
          receivedData.I_1,
          receivedData.I_2,
          receivedData.I_3,
          receivedData.I,
          receivedData.kw_1,
          receivedData.kw_2,
          receivedData.kw_3,
          receivedData.kvar_1,
          receivedData.kvar_2,
          receivedData.kvar_3,
          receivedData.kva_1,
          receivedData.kva_2,
          receivedData.kva_3,
          receivedData.pf_1,
          receivedData.pf_2,
          receivedData.pf_3,
          receivedData.pf,
          receivedData.freq,
          receivedData.kw,
          receivedData.kvar,
          receivedData.kva,
          receivedData.imp_kwh,
          receivedData.exp_kwh,
          receivedData.kwh,
          receivedData.imp_kvarh,
          receivedData.exp_kvarh,
          receivedData.kvarh,
          receivedData.kvah,
          receivedData.thd_v1n,
          receivedData.thd_v2n,
          receivedData.thd_v3n,
          receivedData.thd_v12,
          receivedData.thd_v23,
          receivedData.thd_v31,
          receivedData.thd_i1,
          receivedData.thd_i2,
          receivedData.thd_i3,
          receivedData.max_kw,
          receivedData.min_kw,
          receivedData.max_kvar,
          receivedData.min_kvar,
          receivedData.max_int_v1n,
          receivedData.max_int_v2n,
          receivedData.max_int_v3n,
          receivedData.max_int_v12,
          receivedData.max_int_v23,
          receivedData.max_int_v31,
          receivedData.max_kva,
          receivedData.max_int_i1,
          receivedData.max_int_i2,
          receivedData.max_int_i3,
          receivedData.run_h,
          receivedData.on_h,
          receivedData.ser_no, ]
          pgClient.query(insertQuery,insertValues)
          .then(() => {
            // console.log('Data inserted in db.');
            receivedData = {};
          })
          .catch((error) => {
            console.error('Error inserting data into db',error);
          });
        })

    } else {
        // console.log('Parameters processing...');
        // console.log(receivedData);
    }
});

// error handling for mqtt
mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});
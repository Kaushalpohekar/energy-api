const fs = require('fs');
const { Client } = require('pg');
const json2csv = require('json2csv').parse;

const sourceDBConfig = {
  host: '64.227.181.131',
  user: 'postgres',
  password: 'iotsenselive',
  database: 'senselive_db',
  port: 12440,
};

const sourceDB = new Client(sourceDBConfig);

sourceDB.connect()
  .then(() => console.log('Connected to source database'))
  .catch(err => console.error('Error connecting to source database:', err));

sourceDB.query('SELECT * FROM public.energy_database WHERE device_uid = \'SL02202361\' AND date_time BETWEEN \'2024-04-29 17:03:00\' AND \'2024-05-04 13:00:00\' ORDER BY date_time DESC')
  .then(results => {
    const dataToExport = results.rows;

    const csv = json2csv(dataToExport, { fields: [
      'date_time',
      'device_uid',
      'voltage_1n',
      'voltage_2n',
      'voltage_3n',
      'voltage_n',
      'voltage_12',
      'voltage_23',
      'voltage_31',
      'voltage_l',
      'current_1',
      'current_2',
      'current_3',
      'current',
      'kw_1',
      'kw_2',
      'kw_3',
      'kvar_1',
      'kvar_2',
      'kvar_3',
      'kva_1',
      'kva_2',
      'kva_3',
      'pf_1',
      'pf_2',
      'pf_3',
      'pf',
      'freq',
      'kw',
      'kvar',
      'kva',
      'imp_kwh',
      'exp_kwh',
      'kwh',
      'imp_kvarh',
      'exp_kvarh',
      'kvarh',
      'kvah',
      'thd_v1n',
      'thd_v2n',
      'thd_v3n',
      'thd_v12',
      'thd_v23',
      'thd_v31',
      'thd_i1',
      'thd_i2',
      'thd_i3',
      'max_kw',
      'min_kw',
      'max_kvar',
      'min_kvar',
      'max_int_v1n',
      'max_int_v2n',
      'max_int_v3n',
      'max_int_v12',
      'max_int_v23',
      'max_int_v31',
      'max_kva',
      'max_int_i1',
      'max_int_i2',
      'max_int_i3',
      'run_h',
      'on_h',
      'ser_no'
    ]});

    fs.writeFileSync('exported_data.csv', csv, 'utf8');

    console.log('CSV file generated successfully.');

    sourceDB.end();
  })
  .catch(error => {
    console.error('Error querying source database:', error);
    sourceDB.end();
  });

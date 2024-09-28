const fs = require('fs');
const csv = require('csv-parser');
const { Client } = require('pg');

const client = new Client({
  host: 'pgsql.senselive.in',
  user: 'senselive',
  password: 'SenseLive',
  database: 'ems',
  port: 5432,
});

async function insertRows(rows) {
    const query = `
      INSERT INTO ems_schema.ems_actual_data (
        date_time, device_uid, voltage_1n, voltage_2n, voltage_3n, voltage_n,
        voltage_12, voltage_23, voltage_31, voltage_l, current_1, current_2,
        current_3, current, kw_1, kw_2, kw_3, kvar_1, kvar_2, kvar_3,
        kva_1, kva_2, kva_3, pf_1, pf_2, pf_3, pf, freq, kw, kvar, 
        kva, max_kw, min_kw, max_kvar, min_kvar, max_kva, max_int_v1n,
        max_int_v2n, max_int_v3n, max_int_v12, max_int_v23, max_int_v31,
        max_int_i1, max_int_i2, max_int_i3, imp_kwh, exp_kwh, kwh,
        imp_kvarh, exp_kvarh, kvarh, kvah, run_h, on_h, thd_v1n, 
        thd_v2n, thd_v3n, thd_v12, thd_v23, thd_v31, thd_i1, thd_i2,
        thd_i3, ser_no 
      ) VALUES 
    `;
    
    const values = rows.map((row, index) => {
      const baseIndex = index * 64; // 64 parameters for each row
      return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, 
               $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8}, $${baseIndex + 9}, $${baseIndex + 10}, 
               $${baseIndex + 11}, $${baseIndex + 12}, $${baseIndex + 13}, $${baseIndex + 14}, $${baseIndex + 15},
               $${baseIndex + 16}, $${baseIndex + 17}, $${baseIndex + 18}, $${baseIndex + 19}, $${baseIndex + 20},
               $${baseIndex + 21}, $${baseIndex + 22}, $${baseIndex + 23}, $${baseIndex + 24}, $${baseIndex + 25},
               $${baseIndex + 26}, $${baseIndex + 27}, $${baseIndex + 28}, $${baseIndex + 29}, $${baseIndex + 30},
               $${baseIndex + 31}, $${baseIndex + 32}, $${baseIndex + 33}, $${baseIndex + 34}, $${baseIndex + 35},
               $${baseIndex + 36}, $${baseIndex + 37}, $${baseIndex + 38}, $${baseIndex + 39}, $${baseIndex + 40},
               $${baseIndex + 41}, $${baseIndex + 42}, $${baseIndex + 43}, $${baseIndex + 44}, $${baseIndex + 45},
               $${baseIndex + 46}, $${baseIndex + 47}, $${baseIndex + 48}, $${baseIndex + 49}, $${baseIndex + 50},
               $${baseIndex + 51}, $${baseIndex + 52}, $${baseIndex + 53}, $${baseIndex + 54}, $${baseIndex + 55},
               $${baseIndex + 56}, $${baseIndex + 57}, $${baseIndex + 58}, $${baseIndex + 59}, $${baseIndex + 60},
               $${baseIndex + 61}, $${baseIndex + 62}, $${baseIndex + 63}, $${baseIndex + 64})`;
    }).join(', ');
  
    const params = rows.flatMap(row => [
      row.date_time,
      row.device_uid,
      row.voltage_1n === '' ? null : row.voltage_1n,
      row.voltage_2n === '' ? null : row.voltage_2n,
      row.voltage_3n === '' ? null : row.voltage_3n,
      row.voltage_n === '' ? null : row.voltage_n,
      row.voltage_12 === '' ? null : row.voltage_12,
      row.voltage_23 === '' ? null : row.voltage_23,
      row.voltage_31 === '' ? null : row.voltage_31,
      row.voltage_l === '' ? null : row.voltage_l,
      row.current_1 === '' ? null : row.current_1,
      row.current_2 === '' ? null : row.current_2,
      row.current_3 === '' ? null : row.current_3,
      row.current === '' ? null : row.current,
      row.kw_1 === '' ? null : row.kw_1,
      row.kw_2 === '' ? null : row.kw_2,
      row.kw_3 === '' ? null : row.kw_3,
      row.kvar_1 === '' ? null : row.kvar_1,
      row.kvar_2 === '' ? null : row.kvar_2,
      row.kvar_3 === '' ? null : row.kvar_3,
      row.kva_1 === '' ? null : row.kva_1,
      row.kva_2 === '' ? null : row.kva_2,
      row.kva_3 === '' ? null : row.kva_3,
      row.pf_1 === '' ? null : row.pf_1,
      row.pf_2 === '' ? null : row.pf_2,
      row.pf_3 === '' ? null : row.pf_3,
      row.pf === '' ? null : row.pf,
      row.freq === '' ? null : row.freq,
      row.kw === '' ? null : row.kw,
      row.kvar === '' ? null : row.kvar,
      row.kva === '' ? null : row.kva,
      row.max_kw === '' ? null : row.max_kw,
      row.min_kw === '' ? null : row.min_kw,
      row.max_kvar === '' ? null : row.max_kvar,
      row.min_kvar === '' ? null : row.min_kvar,
      row.max_kva === '' ? null : row.max_kva,
      row.max_int_v1n === '' ? null : row.max_int_v1n,
      row.max_int_v2n === '' ? null : row.max_int_v2n,
      row.max_int_v3n === '' ? null : row.max_int_v3n,
      row.max_int_v12 === '' ? null : row.max_int_v12,
      row.max_int_v23 === '' ? null : row.max_int_v23,
      row.max_int_v31 === '' ? null : row.max_int_v31,
      row.max_int_i1 === '' ? null : row.max_int_i1,
      row.max_int_i2 === '' ? null : row.max_int_i2,
      row.max_int_i3 === '' ? null : row.max_int_i3,
      row.imp_kwh === '' ? null : row.imp_kwh,
      row.exp_kwh === '' ? null : row.exp_kwh,
      row.kwh === '' ? null : row.kwh,
      row.imp_kvarh === '' ? null : row.imp_kvarh,
      row.exp_kvarh === '' ? null : row.exp_kvarh,
      row.kvarh === '' ? null : row.kvarh,
      row.kvah === '' ? null : row.kvah,
      row.run_h === '' ? null : row.run_h,
      row.on_h === '' ? null : row.on_h,
      row.thd_v1n === '' ? null : row.thd_v1n,
      row.thd_v2n === '' ? null : row.thd_v2n,
      row.thd_v3n === '' ? null : row.thd_v3n,
      row.thd_v12 === '' ? null : row.thd_v12,
      row.thd_v23 === '' ? null : row.thd_v23,
      row.thd_v31 === '' ? null : row.thd_v31,
      row.thd_i1 === '' ? null : row.thd_i1,
      row.thd_i2 === '' ? null : row.thd_i2,
      row.thd_i3 === '' ? null : row.thd_i3,
      row.ser_no === '' ? null : row.ser_no,
    ]);
  
    await client.query(query + values, params);
  }

async function importCsv(filePath) {
  await client.connect();

  try {
    await client.query('BEGIN');

    let totalRows = 0;
    let processedRows = 0;
    const batchSize = 100;

    const calculateTotalRows = new Promise((resolve) => {
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', () => {
          totalRows++;
        })
        .on('end', () => {
          console.log(`Total rows: ${totalRows}`);
          resolve();
        });
    });

    await calculateTotalRows;

    const processRows = new Promise((resolve, reject) => {
      const stream = fs.createReadStream(filePath)
        .pipe(csv());

      let rowsBatch = [];

      stream.on('data', async (row) => {
        rowsBatch.push(row);

        if (rowsBatch.length === batchSize) {
          stream.pause();

          try {
            await insertRows(rowsBatch);
            processedRows += rowsBatch.length;
            const progress = ((processedRows / totalRows) * 100).toFixed(2);
            console.log(`Progress: ${progress}%`);
            rowsBatch = [];
            stream.resume();
          } catch (err) {
            console.error('Error inserting rows, rolling back...', err);
            stream.destroy();
            reject(err);
          }
        }
      });

      stream.on('end', async () => {
        if (rowsBatch.length > 0) {
          await insertRows(rowsBatch);
          processedRows += rowsBatch.length;
        }
        resolve();
      });
    });

    await processRows;

    await client.query('COMMIT');
    console.log('CSV import completed and committed.');
  } catch (err) {
    console.error('Transaction error, rolling back...', err);
    await client.query('ROLLBACK');
  } finally {
    await client.end();
  }
}

importCsv('C:/Users/uder/Desktop/ems_actual_data_202409271651.csv');

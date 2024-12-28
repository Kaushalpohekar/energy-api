const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql2/promise');

const dbConfig = {
  host: 'sl02-mysql.mysql.database.azure.com',
  user: 'senselive',
  password: 'SenseLive@2030',
  database: 'tms',
  ssl: {
    rejectUnauthorized: false, 
  }
};

async function insertRows(connection, rows) {
  const query = `
    INSERT INTO actual_data (
      TimeStamp, DeviceUID, Temperature, Humidity, TemperatureR,
      TemperatureY, TemperatureB, Pressure, flowRate, totalVolume,
      ip_address, status
    ) VALUES ?
  `;

  const values = rows.map(row => [
    row.TimeStamp,                  
    row.DeviceUID,                 
    row.Temperature || null,        
    row.Humidity || null,           
    row.TemperatureR || null,      
    row.TemperatureY || null,      
    row.TemperatureB || null,      
    row.Pressure || null,           
    row.flowRate || null,          
    row.totalVolume || null,       
    row.ip_address || null,         
    row.status || null              
  ]);

  //console.log(`Inserting batch of ${rows.length} rows into database.`);
  await connection.query(query, [values]);
  //console.log(`Batch insertion successful.`);
}

async function importCsv(filePath) {
  const connection = await mysql.createConnection(dbConfig);

  try {
    console.log('Transaction starting...');
    await connection.beginTransaction();

    let totalRows = 0;
    let processedRows = 0;
    const batchSize = 100;

    console.log(`Calculating total rows in file: ${filePath}`);
    const calculateTotalRows = new Promise(resolve => {
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', () => totalRows++)
        .on('end', () => {
          console.log(`Total rows found: ${totalRows}`);
          resolve();
        });
    });

    await calculateTotalRows;

    const processRows = new Promise((resolve, reject) => {
      const stream = fs.createReadStream(filePath)
        .pipe(csv());

      let rowsBatch = [];

      stream.on('data', async row => {
        rowsBatch.push(row);

        if (rowsBatch.length === batchSize) {
          stream.pause();

          //console.log('Processing a batch...');
          try {
            await insertRows(connection, rowsBatch);
            processedRows += rowsBatch.length;
            const progress = ((processedRows / totalRows) * 100).toFixed(2);
            console.log(`Progress: ${progress}% (${processedRows}/${totalRows})`);
            rowsBatch = [];
            stream.resume();
          } catch (err) {
            console.error('Error inserting batch, rolling back transaction.', err);
            await connection.rollback();
            stream.destroy();
            reject(err);
          }
        }
      });

      stream.on('end', async () => {
        if (rowsBatch.length > 0) {
          //console.log('Processing the last batch...');
          await insertRows(connection, rowsBatch);
          processedRows += rowsBatch.length;
        }
        resolve();
      });

      stream.on('error', err => {
        console.error('Error reading CSV file.', err);
        reject(err);
      });
    });

    await processRows;

    //console.log('All batches processed. Committing transaction...');
    await connection.commit();
    //console.log('CSV import completed and committed successfully.');
  } catch (err) {
    console.error('Transaction failed, rolling back...', err);
    await connection.rollback();
  } finally {
    //console.log('Closing database connection...');
    await connection.end();
    console.log('Database connection closed.');
  }
}

const filePath = 'C:/Users/uder/Desktop/actual_data_202412271202.csv';
console.log(`Starting CSV import for file: ${filePath}`);
importCsv(filePath).then(() => {
  console.log('CSV import process completed.');
}).catch(err => {
  console.error('CSV import process encountered an error.', err);
});

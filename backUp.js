const fs = require('fs');
const mysql = require('mysql2/promise');
const json2csv = require('json2csv').parse;

const sourceDBConfig = {
  host: 'senso.senselive.in',
  user: 'mysql',
  password: 'sense!123',
  database: 'tms',
  port: 3306, // Adjust the port if necessary
};

async function exportData() {
  let sourceDB;
  try {
    sourceDB = await mysql.createConnection(sourceDBConfig);
    console.log('Connected to source database');
    
    // Adjust the query to select data between January 1st to March 31st, 2024
    const [rows, fields] = await sourceDB.execute(`
      SELECT TimeStamp, DeviceUID, Temperature,Humidity,TemperatureR,TemperatureY,TemperatureB,flowRate,totalVolume,Pressure
      FROM actual_data
      WHERE TimeStamp >= '2024-06-01 00:00:00' AND TimeStamp <= '2024-06-30 23:59:59'
      ORDER BY TimeStamp DESC
    `);

    const dataToExport = rows;

    const csv = json2csv(dataToExport, {
      fields: [
        'TimeStamp',
        'DeviceUID',
        'Temperature',
        'Humidity',
        'TemperatureR',
        'TemperatureY',
        'TemperatureB',
        'flowRate',
        'totalVolume',
        'Pressure',
      ],
    });

    fs.writeFileSync('backup_01-01_to_03-31-2024.csv', csv, 'utf8');

    console.log('CSV file generated successfully.');
  } catch (error) {
    console.error('Error querying source database:', error);
  } finally {
    if (sourceDB) {
      await sourceDB.end();
    }
  }
}

exportData();

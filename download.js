const mysql = require('mysql2/promise');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

async function queryDatabaseAndSaveToCsv() {
    const dbConfig = {
        host: 'senso.senselive.in',
        user: 'mysql',
        password: 'sense!123',
        database: 'tms'
    };

    const query = `
        SELECT 
            DeviceUID, 
            DATE_FORMAT(TimeStamp, '%Y-%m-%d %H:%i:%s') AS TimeStamp, 
            Temperature, 
            Humidity, 
            flowRate, 
            TemperatureR, 
            TemperatureY, 
            TemperatureB, 
            Pressure, 
            totalVolume 
        FROM tms.actual_data 
        WHERE TimeStamp BETWEEN '2024-11-01 00:00:00' AND '2024-12-01'
    `;

    const outputFilePath = 'output.csv';

    let connection;

    try {
        connection = await mysql.createConnection(dbConfig);
        console.log('Database connection successful.');

        const [rows] = await connection.execute(query);
        console.log(`Total rows fetched: ${rows.length}`);

        if (rows.length === 0) {
            console.log('No data to write to CSV.');
            return;
        }

        const csvWriter = createCsvWriter({
            path: outputFilePath,
            header: Object.keys(rows[0]).map((key) => ({ id: key, title: key })),
        });

        await csvWriter.writeRecords(rows);
        console.log(`Data successfully saved to ${outputFilePath}`);
    } catch (error) {
        console.error('An error occurred:', error);
    } finally {
        if (connection) {
            await connection.end();
            console.log('Database connection closed.');
        }
    }
}

queryDatabaseAndSaveToCsv();

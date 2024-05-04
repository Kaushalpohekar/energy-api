const fs = require('fs');
const mysql = require('mysql'); // Assuming MySQL database
const { promisify } = require('util');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Create a MySQL connection
const connection = mysql.createConnection({
    host: '13.200.38.129',
    user: 'mysql',
    password: 'sense!123',
    database: 'tms'
});

// Promisify the connection.query method
const query = promisify(connection.query).bind(connection);

async function fetchJanuaryData() {
    try {
        // Assuming the table name is 'january_table'
        //const sql = 'SELECT * FROM actual_data where TimeStamp Between "2024-01-01" AND "2024-01-31";';
        const sql = 'SELECT DeviceUID, Temperature, TemperatureR, TemperatureY, TemperatureY, TemperatureB, Humidity, flowRate, totalVolume, DATE_FORMAT(TimeStamp, "%Y-%m-%d %H:%i:%s") AS TimeStamp, ip_address, status, Pressure FROM actual_data WHERE TimeStamp BETWEEN "2024-02-01" AND "2024-02-31";';

        const data = await query(sql);
        return data;
    } catch (error) {
        console.error('Error fetching January data:', error);
        throw error;
    }
}

async function exportToCSV(data) {
    const columnNames = Object.keys(data[0]); // Assuming data is not empty

    const csvWriter = createCsvWriter({
        path: 'Feb_data.csv',
        header: columnNames.map(name => ({ id: name, title: name }))
    });

    try {
        await csvWriter.writeRecords(data);
        console.log('CSV file has been written successfully');
    } catch (error) {
        console.error('Error exporting to CSV:', error);
        throw error;
    }
}

async function main() {
    try {
        const januaryData = await fetchJanuaryData();
        await exportToCSV(januaryData);
        connection.end(); // Close the database connection
    } catch (error) {
        console.error('An error occurred:', error);
    }
}

main();

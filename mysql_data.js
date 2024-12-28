const mysql = require('mysql2/promise');
const { Parser } = require('json2csv');
const fs = require('fs');

const dbConfig = {
    //   host: 'sl02-mysql.mysql.database.azure.com',
    //   user: 'senselive',
    //   password: 'SenseLive@2030',
    //   database: 'tms',
    //   ssl: { rejectUnauthorized: false },
    host: 'senso.senselive.in',
    user: 'mysql',
    password: 'sense!123',
    database: 'tms'
};

// async function exportDataToCsv(filePath) {
//     const connection = await mysql.createConnection(dbConfig);
//     const batchSize = 2000; // Export data in batches
//     const fields = [
//         'TimeStamp', 'DeviceUID', 'Temperature', 'Humidity', 'TemperatureR',
//         'TemperatureY', 'TemperatureB', 'Pressure', 'flowRate', 'totalVolume',
//         'ip_address', 'status'
//     ];

//     const json2csv = new Parser({ fields, header: true });
//     const writeStream = fs.createWriteStream(filePath);

//     try {
//         console.log('Connecting to database...');

//         // Count total rows for progress tracking
//         const [rowCount] = await connection.execute('SELECT COUNT(*) AS total FROM actual_data');
//         const totalRows = rowCount[0].total;
//         console.log(`Total rows to export: ${totalRows}`);

//         let offset = 0;
//         let processedRows = 0;

//         while (offset < totalRows) {
//             // Dynamically construct the query
//             const query = `SELECT * FROM actual_data LIMIT ${batchSize} OFFSET ${offset}`;

//             // Fetch data in batches
//             const [rows] = await connection.query(query);

//             // Convert rows to CSV format
//             let csv;
//             if (offset === 0) {
//                 // Include headers for the first batch
//                 csv = json2csv.parse(rows);
//             } else {
//                 // Skip headers for subsequent batches
//                 const csvParserNoHeader = new Parser({ fields, header: false });
//                 csv = csvParserNoHeader.parse(rows);
//             }

//             writeStream.write(`${csv}\n`);

//             processedRows += rows.length;
//             const progress = ((processedRows / totalRows) * 100).toFixed(2);
//             console.log(`Progress: ${progress}% (${processedRows}/${totalRows})`);

//             offset += batchSize;
//         }

//         console.log('Export completed successfully.');
//     } catch (err) {
//         console.error('An error occurred during export:', err);
//     } finally {
//         writeStream.close();
//         await connection.end();
//         console.log('Database connection closed.');
//     }
// }
async function exportDataToCsv(filePath) {
    const connection = await mysql.createConnection(dbConfig);
    const batchSize = 2000; // Export data in batches
    const fields = [
        'TimeStamp', 'DeviceUID', 'Temperature', 'Humidity', 'TemperatureR',
        'TemperatureY', 'TemperatureB', 'Pressure', 'flowRate', 'totalVolume',
        'ip_address', 'status'
    ];

    const json2csv = new Parser({ fields, header: true });
    const writeStream = fs.createWriteStream(filePath);

    try {
        console.log('Connecting to database...');

        // Set the date range for the export
        const startDate = '2024-12-10';
        const endDate = new Date().toISOString().slice(0, 19).replace('T', ' '); // Current date and time (in 'YYYY-MM-DD HH:MM:SS' format)

        // Count total rows for progress tracking
        const [rowCount] = await connection.execute(
            `SELECT COUNT(*) AS total FROM actual_data WHERE TimeStamp BETWEEN ? AND ?`,
            [startDate, endDate]
        );
        const totalRows = rowCount[0].total;
        console.log(`Total rows to export: ${totalRows}`);

        let offset = 0;
        let processedRows = 0;

        while (offset < totalRows) {
            // Dynamically construct the query with the date filter
            const query = `
                SELECT * FROM actual_data
                WHERE TimeStamp BETWEEN ? AND ?
                LIMIT ${batchSize} OFFSET ${offset}
            `;

            // Fetch data in batches
            const [rows] = await connection.query(query, [startDate, endDate]);

            // Convert rows to CSV format
            let csv;
            if (offset === 0) {
                // Include headers for the first batch
                csv = json2csv.parse(rows);
            } else {
                // Skip headers for subsequent batches
                const csvParserNoHeader = new Parser({ fields, header: false });
                csv = csvParserNoHeader.parse(rows);
            }

            writeStream.write(`${csv}\n`);

            processedRows += rows.length;
            const progress = ((processedRows / totalRows) * 100).toFixed(2);
            console.log(`Progress: ${progress}% (${processedRows}/${totalRows})`);

            offset += batchSize;
        }

        console.log('Export completed successfully.');
    } catch (err) {
        console.error('An error occurred during export:', err);
    } finally {
        writeStream.close();
        await connection.end();
        console.log('Database connection closed.');
    }
}

const filePath = 'E:/Project/energy-api/output_export.csv';
console.log(`Starting data export to file: ${filePath}`);
exportDataToCsv(filePath)
    .then(() => console.log('Data export process completed.'))
    .catch(err => console.error('Data export process encountered an error:', err));

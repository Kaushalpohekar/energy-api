// const mysql = require('mysql2/promise');
// const { Parser } = require('json2csv');
// const fs = require('fs');

// const dbConfig = {
//     //   host: 'sl02-mysql.mysql.database.azure.com',
//     //   user: 'senselive',
//     //   password: 'SenseLive@2030',
//     //   database: 'tms',
//     //   ssl: { rejectUnauthorized: false },
//     host: 'senso.senselive.in',
//     user: 'mysql',
//     password: 'sense!123',
//     database: 'tms'
// };

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

// const filePath = 'E:/Project/energy-api/output_export.csv';
// console.log(`Starting data export to file: ${filePath}`);
// exportDataToCsv(filePath)
//     .then(() => console.log('Data export process completed.'))
//     .catch(err => console.error('Data export process encountered an error:', err));
const mysql = require('mysql2/promise');
const { Parser } = require('json2csv');
const fs = require('fs');

const dbConfig = {
    host: 'senso.senselive.in',
    user: 'mysql',
    password: 'sense!123',
    database: 'tms'
};

async function exportDataToCsv(filePath) {
    const connection = await mysql.createConnection(dbConfig);
    const batchSize = 2000; // Export data in batches
    let fields = null; // Fields will be determined dynamically if not defined
    const writeStream = fs.createWriteStream(filePath);

    try {
        console.log('Connecting to database...');
        writeStream.write(''); // Initialize the file to prevent issues

        // Define the start and end dates for the range
        const startDate = new Date('2023-07-01');
        const endDate = new Date('2023-12-01');

        let currentStartDate = new Date(startDate);
        let currentEndDate = new Date(currentStartDate);
        currentEndDate.setDate(currentEndDate.getDate() + 5);

        let totalProcessedRows = 0;

        while (currentStartDate < endDate) {
            console.log(`Processing data from ${currentStartDate.toISOString()} to ${currentEndDate.toISOString()}...`);

            // Count rows for the current 5-day interval
            const [rowCount] = await connection.execute(`
                SELECT COUNT(*) AS total FROM actual_data 
                WHERE TimeStamp >= ? AND TimeStamp < ? AND DeviceUID IS NOT NULL
            `, [currentStartDate.toISOString(), currentEndDate.toISOString()]);

            const intervalTotalRows = rowCount[0].total;
            console.log(`Total rows to export in this interval: ${intervalTotalRows}`);

            if (intervalTotalRows === 0) {
                // Skip this interval if no data exists
                currentStartDate = new Date(currentEndDate);
                currentEndDate.setDate(currentEndDate.getDate() + 5);
                continue;
            }

            let offset = 0;
            let processedRows = 0;

            while (processedRows < intervalTotalRows) {
                // Query data for the current date range and in batches
                const query = `
                    SELECT * FROM actual_data 
                    WHERE TimeStamp >= ? AND TimeStamp < ? AND DeviceUID IS NOT NULL 
                    LIMIT ? OFFSET ?`;

                const [rows] = await connection.execute(query, [
                    currentStartDate.toISOString(),
                    currentEndDate.toISOString(),
                    batchSize,
                    offset
                ]);

                if (rows.length === 0) break; // No more rows to process in this batch

                // Dynamically set fields if not already defined
                if (!fields) {
                    fields = Object.keys(rows[0]);
                    const json2csvHeader = new Parser({ fields, header: true });
                    writeStream.write(json2csvHeader.getHeader() + '\n'); // Write header once
                }

                // Convert rows to CSV format
                const json2csv = new Parser({ fields, header: false });
                const csv = json2csv.parse(rows);
                writeStream.write(`${csv}\n`);

                processedRows += rows.length;
                totalProcessedRows += rows.length;

                // Calculate progress for the current interval
                const progress = ((processedRows / intervalTotalRows) * 100).toFixed(2);
                console.log(`Interval Progress: ${progress}% (${processedRows}/${intervalTotalRows})`);

                offset += batchSize;
            }

            // Move to the next 5-day interval
            currentStartDate = new Date(currentEndDate);
            currentEndDate.setDate(currentEndDate.getDate() + 5);
        }

        console.log(`Export completed successfully. Total rows exported: ${totalProcessedRows}`);
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

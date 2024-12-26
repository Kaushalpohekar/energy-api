const fs = require('fs');
const csvParser = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');

// Input file paths
const file1 = 'C:/Users/uder/Desktop/actual_data_202412231650.csv';
const file2 = 'E:/Project/energy-api/output.csv';

// Output file path
const outputFile = 'merged.csv';

// Function to read CSV and return data as an array of objects
const readCSV = (filePath) => {
    return new Promise((resolve, reject) => {
        const data = [];
        fs.createReadStream(filePath)
            .pipe(csvParser())
            .on('data', (row) => data.push(row))
            .on('end', () => resolve(data))
            .on('error', (error) => reject(error));
    });
};

(async () => {
    try {
        // Read data from both CSV files
        const data1 = await readCSV(file1);
        const data2 = await readCSV(file2);

        // Merge data from both files
        const mergedData = [...data1, ...data2];

        if (mergedData.length === 0) {
            console.log('No data to write. Both files are empty.');
            return;
        }

        // Get column headers dynamically from the first row
        const headers = Object.keys(mergedData[0]);

        // Create a CSV writer
        const csvWriter = createObjectCsvWriter({
            path: outputFile,
            header: headers.map((header) => ({ id: header, title: header })),
        });

        // Write the merged data to the output file
        await csvWriter.writeRecords(mergedData);
        console.log(`CSV files successfully merged into '${outputFile}'`);
    } catch (error) {
        console.error('Error merging CSV files:', error);
    }
})();

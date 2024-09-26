const { Client } = require('pg');
const express = require('express');
const app = express();
const port = 3000;

const sourceDbConfig = {
    host: '3.110.101.216',
    user: 'postgres',
    password: 'sense123',
    database: 'ems',
    port: 5432,
};

const destinationDbConfig = {
    host: 'pgsql.senselive.in',
    user: 'senselive',
    password: 'SenseLive',
    database: 'ems',
    port: 5432,
};

async function transferData(batchSize = 1000, progressCallback, timeCallback) {
    const sourceClient = new Client(sourceDbConfig);
    const destinationClient = new Client(destinationDbConfig);

    try {
        await sourceClient.connect();
        await destinationClient.connect();
        console.log('Connected to source and destination databases');

        const totalRes = await sourceClient.query('SELECT COUNT(*) FROM ems_schema.ems_actual_data');
        const totalEntries = parseInt(totalRes.rows[0].count);
        let transferred = 0;
        const startTime = Date.now();
        console.log(`Total entries to transfer: ${totalEntries}`);

        for (let offset = 0; offset < totalEntries; offset += batchSize) {
            await destinationClient.query('BEGIN');

            try {
                const result = await sourceClient.query(
                    `SELECT * FROM ems_schema.ems_actual_data ORDER BY id LIMIT ${batchSize} OFFSET ${offset}`
                );

                const batchData = result.rows.map(row => {
                    const { id, ...dataWithoutId } = row;

                    Object.keys(dataWithoutId).forEach(key => {
                        if (dataWithoutId[key] instanceof Date) {
                            dataWithoutId[key] = dataWithoutId[key].toISOString();
                        }
                    });

                    return dataWithoutId;
                });

                for (const row of batchData) {
                    const fields = Object.keys(row).join(',');
                    const values = Object.values(row).map(val =>
                        val === null || val === undefined ? 'NULL' : `'${val}'`
                    ).join(',');

                    try {
                        await destinationClient.query(
                            `INSERT INTO ems_schema.ems_actual_data (${fields}) VALUES (${values})`
                        );
                    } catch (err) {
                        console.error('Error during batch insert:', err);
                    }
                }

                await destinationClient.query('COMMIT');
                transferred += result.rowCount;

                const elapsedTime = (Date.now() - startTime) / 1000; // seconds
                const progress = (transferred / totalEntries) * 100;
                const remainingTime = ((elapsedTime / transferred) * (totalEntries - transferred)).toFixed(2);

                progressCallback(progress);
                timeCallback(remainingTime);

                console.log(`Transferred ${transferred} out of ${totalEntries} entries`);
            } catch (err) {
                await destinationClient.query('ROLLBACK');
                console.error('Error during batch transfer:', err);
            }
        }
    } catch (err) {
        console.error('Error connecting to databases:', err);
    } finally {
        await sourceClient.end();
        await destinationClient.end();
        console.log('Database connections closed');
    }
}

app.get('/', (req, res) => {
    res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Transfer Progress</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body>
        <div class="container mt-5">
            <h1>Data Transfer</h1>
            <button id="startButton" class="btn btn-primary" onclick="startTransfer()">Start Data Transfer</button>
            <div class="progress mt-3">
                <div id="progressBar" class="progress-bar" role="progressbar" style="width: 0%;" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">0%</div>
            </div>
            <p id="estimatedTime">Estimated time remaining: Calculating...</p>
        </div>

        <script>
            function startTransfer() {
                document.getElementById('startButton').disabled = true;
                fetch('/start-transfer')
                    .then(response => response.json())
                    .then(data => {
                        console.log('Data transfer started');
                    })
                    .catch(error => console.error('Error starting transfer:', error));
            }

            const eventSource = new EventSource('/progress');
            eventSource.onmessage = function(event) {
                const data = JSON.parse(event.data);
                const progress = parseFloat(data.progress);
                const remainingTime = data.remainingTime;

                const progressBar = document.getElementById('progressBar');
                progressBar.style.width = progress + '%';
                progressBar.innerText = progress.toFixed(2) + '%';
                document.getElementById('estimatedTime').innerText = 'Estimated time remaining: ' + remainingTime + ' seconds';
            };
        </script>
    </body>
    </html>
    `);
});

app.get('/start-transfer', (req, res) => {
    res.json({ message: 'Transfer started' });

    transferData(1000, progress => {
        currentProgress = progress;
    }, time => {
        estimatedTime = time;
    });
});

let currentProgress = 0;
let estimatedTime = 0; // Define remainingTime globally

app.get('/progress', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const interval = setInterval(() => {
        res.write(`data: ${JSON.stringify({ progress: currentProgress.toFixed(2), remainingTime: estimatedTime })}\n\n`);
        if (currentProgress >= 100) {
            clearInterval(interval);
        }
    }, 1000);
});

app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});

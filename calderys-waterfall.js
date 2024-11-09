const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const dotenv = require('dotenv');
dotenv.config();

const app = express();
const port = 9000;

app.use(cors());
app.use(express.json());

const pool = new Pool({
    host: 'pgsql.senselive.in',
    database: 'ems',
    user: 'senselive',
    password: 'SenseLive',
    port: 5432
});

app.get('/get-data', async (req, res) => {
    const { start_date, end_date } = req.query;
    if (!start_date || !end_date) return res.status(400).json({ error: 'Start date and end date are required' });

    try {
        const query = `
            SELECT 
                ((SUM(changeover_time) / (SUM(testing_time) + SUM(changeover_time) + SUM(breakdown_time) + 
                SUM(qc_others) + SUM(setup_time) + SUM(other_stoppages))) * 100) AS "Changeover Time",
                ((SUM(setup_time) / (SUM(testing_time) + SUM(changeover_time) + SUM(breakdown_time) + 
                SUM(qc_others) + SUM(setup_time) + SUM(other_stoppages))) * 100) AS "Setup Time",
                ((SUM(testing_time) / (SUM(testing_time) + SUM(changeover_time) + SUM(breakdown_time) + 
                SUM(qc_others) + SUM(setup_time) + SUM(other_stoppages))) * 100) AS "Testing Time",
                ((SUM(breakdown_time) / (SUM(testing_time) + SUM(changeover_time) + SUM(breakdown_time) + 
                SUM(qc_others) + SUM(setup_time) + SUM(other_stoppages))) * 100) AS "Breakdown Time",    
                ((SUM(other_stoppages) / (SUM(testing_time) + SUM(changeover_time) + SUM(breakdown_time) + 
                SUM(qc_others) + SUM(setup_time) + SUM(other_stoppages))) * 100) AS "Other Stoppages",
                ((SUM(qc_others) / (SUM(testing_time) + SUM(changeover_time) + SUM(breakdown_time) + 
                SUM(qc_others) + SUM(setup_time) + SUM(other_stoppages))) * 100) AS "QC Others"
            FROM 
                clayders.clayders_oee
            WHERE 
                start_date BETWEEN $1 AND $2
        `;

        const values = [start_date, end_date];
        const result = await pool.query(query, values);
        const data = result.rows[0];

        const chartData = Object.entries(data).map(([label, value]) => ({
            x: label,
            value: parseFloat(value).toFixed(2)
        }));

        chartData.push({
            x: 'Total',
            value: "0",
            isTotal: true
        });

        res.json(chartData);
    } catch (error) {
        console.error('Error executing query', error.stack);
        res.status(500).json({ error: 'Database error', details: error.message });
    }
});

app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});

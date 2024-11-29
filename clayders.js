const { Pool } = require('pg');
const cron = require('cron');

// Source Database Connection
const sourcePool = new Pool({
  user: 'postgres',
  host: '3.110.101.216',
  database: 'SWP',
  password: 'sense123',
  port: 5432,
});

// Destination Database Connection
const destPool = new Pool({
  user: 'senselive',
  host: 'pgsql.senselive.in',
  database: 'ems',
  password: 'SenseLive',
  port: 5432,
});

// Function to fetch data from source database
const fetchSourceData = async () => {
  const query = `
    SELECT
      s.submission_id,
      s.start_date + s.start_time AS "start_date",
      s.end_date + s.end_time AS "end_date",
      s.status,
      u1.first_name || ' ' || u1.last_name AS "requested_by",
      u2.first_name || ' ' || u2.last_name AS "authorizer",
      MAX(CASE WHEN q.question_id = 'cdabe603-7ec3-4457-9929-8ef84cbab385' THEN a.answer_text END) AS "Shift",
      MAX(CASE WHEN q.question_id = 'a37b52af-4b71-4edb-b06e-16cba016e334' THEN a.answer_text END) AS "Shift Operator",
      MAX(CASE WHEN q.question_id = 'eaf9cbd4-d6e9-48e9-b06c-6c69c0766cba' THEN a.answer_text END) AS "Headcount (in Nos.)",
      MAX(CASE WHEN q.question_id = '035ba28e-38c3-4e32-9c16-58a5c1b20159' THEN a.answer_text END) AS "Planned Production (in MT)",
      MAX(CASE WHEN q.question_id = 'b27a5398-664d-473c-bedd-6993ab682b6a' THEN a.answer_text END) AS "Actual Production (in MT)",
      MAX(CASE WHEN q.question_id = '658c6f9c-4805-46ce-a87f-2d975b10b3ac' THEN a.answer_text END) AS "Machine Running Hours (in hours)",
      MAX(CASE WHEN q.question_id = '7b283330-885d-48e4-8267-606c7766db5b' THEN a.answer_text END) AS "Energy Consumption (in KWH)",
      MAX(CASE WHEN q.question_id = 'c4af439d-e238-4271-a79f-5dcca3ece4bd' THEN a.answer_text END) AS "No. of Product Changeover (in Nos)",
      MAX(CASE WHEN q.question_id = 'afebe4da-e4b6-467a-a26d-0b8fbf67c041' THEN a.answer_text END) AS "Changeover Time (in hours)",
      MAX(CASE WHEN q.question_id = '7205bb80-992d-4f67-ba86-7666c24519b1' THEN a.answer_text END) AS "Setup Time (in hours)",
      MAX(CASE WHEN q.question_id = 'e34011e0-db18-4a0e-882d-44fa58c9d3a1' THEN a.answer_text END) AS "Breakdown Time (in hours)",
      MAX(CASE WHEN q.question_id = '3ba1c41b-7c97-43d2-8aad-dcbd2fa57672' THEN a.answer_text END) AS "Testing Time (in hours)",
      MAX(CASE WHEN q.question_id = '686eeb61-b9e8-4966-8a2b-cf5fc7730bef' THEN a.answer_text END) AS "Unavailability of Raw Material (in hours)",
      MAX(CASE WHEN q.question_id = 'a6b6dd15-b5c9-429e-b920-ea94b12ee7d6' THEN a.answer_text END) AS "QC others (in hours)",
      MAX(CASE WHEN q.question_id = '5cf74c60-296f-45df-a846-e4e766667e44' THEN a.answer_text END) AS "Other Stoppages (in hours)",
      MAX(CASE WHEN q.question_id = 'b8fbae92-8bbb-4338-b7d8-930291bcbfbd' THEN a.answer_text END) AS "Loss Due To Absence of Worker (in MT)",
      MAX(CASE WHEN q.question_id = 'cf21e321-890c-46c6-9218-3263cbd3ada1' THEN a.answer_text END) AS "Rejected Production (in MT)",
      MAX(CASE WHEN q.question_id = '9190ec6b-f724-4af3-ab37-cecf6893340d' THEN a.answer_text END) AS "Reprocess Material (in MT)"
    FROM public.submissions s
    JOIN public.users u1 ON s.requested_by = u1.user_id
    JOIN public.users u2 ON s.authorizer = u2.user_id
    LEFT JOIN public.questions q ON q.form_id = s.form_id
    LEFT JOIN public.answers a ON a.submission_id = s.submission_id AND a.question_id = q.question_id
    WHERE s.form_id = 'd891e2d8-eded-4688-8936-ee79c8b8d562'
      AND s.status = 'approved'
      AND s.created_at >= NOW() - INTERVAL '1000 HOURS'
    GROUP BY s.submission_id, s.start_date, s.start_time, s.end_date, s.end_time, s.status, u1.first_name, u1.last_name, u2.first_name, u2.last_name
    ORDER BY s.start_date;
  `;

  const { rows } = await sourcePool.query(query);
  return rows;
};

// Function to insert data into the destination database
const insertIntoDest = async (data) => {
  for (const row of data) {
    const checkQuery = `SELECT 1 FROM clayders.clayders_oee WHERE submission_id = $1`;
    const checkResult = await destPool.query(checkQuery, [row.submission_id]);

    if (checkResult.rowCount === 0) {
      // If not already present, insert the row
      const insertQuery = `
        INSERT INTO clayders.clayders_oee (
          submission_id, start_date, end_date, status, requested_by, authorizer, shift, shift_operator,
          headcount, planned_production, actual_production, machine_running_hours, energy_consumption,
          product_changeovers, changeover_time, setup_time, breakdown_time, testing_time, raw_material_unavailability,
          qc_others, other_stoppages, loss_due_to_absence_of_worker, rejected_production, reprocess_material
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
        )`;
        const values = [
            row.submission_id || null,
            row.start_date || null,
            row.end_date || null,
            row.status || null,
            row.requested_by || null,
            row.authorizer || null,
            row.Shift || null,
            row["Shift Operator"] || null,
            row["Headcount (in Nos.)"] || null,
            row["Planned Production (in MT)"] || null,
            row["Actual Production (in MT)"] || null,
            row["Machine Running Hours (in hours)"] || null,
            row["Energy Consumption (in KWH)"] || null,
            row["No. of Product Changeover (in Nos)"] || null,
            row["Changeover Time (in hours)"] || null,
            row["Setup Time (in hours)"] || null,
            row["Breakdown Time (in hours)"] || null,
            row["Testing Time (in hours)"] || null,
            row["Unavailability of Raw Material (in hours)"] || null,
            row["QC others (in hours)"] || null,
            row["Other Stoppages (in hours)"] || null,
            row["Loss Due To Absence of Worker (in MT)"] || null,
            row["Rejected Production (in MT)"] || null,
            row["Reprocess Material (in MT)"] || null
          ];          
      await destPool.query(insertQuery, values);
      console.log(`Data inserted for submission_id: ${row.submission_id}`);
    } else {
        //console.log("Data Avilable", checkResult.rowCount);
        //console.log(`Data already available for submission_id: ${row.submission_id}`);
    }
  } 
};

// Set up the cron job to run every 10 minutes
// const job = new cron.CronJob('*/10 * * * *', async () => {
//   console.log('Running cron job to fetch and insert data...');
//   try {
//     const data = await fetchSourceData();
//     await insertIntoDest(data);
//     console.log('Data inserted successfully.');
//   } catch (error) {
//     console.error('Error during cron job execution:', error);
//   }
// });


// Set up the cron job to run every 5 seconds
const job = new cron.CronJob('*/10 * * * * *', async () => {
    console.log('Running cron job to fetch and insert data...');
    try {
      const data = await fetchSourceData();
      await insertIntoDest(data);
      console.log('Data inserted successfully.');
    } catch (error) {
      console.error('Error during cron job execution:', error);
    }
  });
  
// Start the cron job
job.start();

const { Pool } = require('pg');
const cron = require('cron');

// Destination Database Connection
const sourcePool = new Pool({
  host: 'data.senselive.in',
  user: 'senselive',
  password: 'SenseLive@2025',
  database: 'senselive_db',
  port: 5432,
  ssl: { rejectUnauthorized: false },
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
      MAX(CASE WHEN q.question_id = '88f7420c-e4e4-4b7d-b124-2e5e6245f8d5' THEN a.answer_text END) AS "Shift",
      MAX(CASE WHEN q.question_id = '4f5e52eb-97df-412a-8bfa-87a3eb3594cb' THEN a.answer_text END) AS "Shift Operator",
      MAX(CASE WHEN q.question_id = 'a86d50f5-57a2-477e-9063-9d933d8157af' THEN a.answer_text END) AS "Headcount (in Nos.)",
      MAX(CASE WHEN q.question_id = 'aa4c7b33-7de5-4991-84a4-adb3ad6301be' THEN a.answer_text END) AS "Tonnes/Manpower",
      MAX(CASE WHEN q.question_id = '3f1623eb-3990-4140-97e6-eb2c28dfa2ef' THEN a.answer_text END) AS "product/Batch Selection",
      MAX(CASE WHEN q.question_id = '6a545e4b-cd0e-43f5-97d4-44fdcd3e4644' THEN a.answer_text END) AS "Planned Production (in MT)",
      MAX(CASE WHEN q.question_id = 'd85b818e-5c10-4f71-9763-e7608b1da45d' THEN a.answer_text END) AS "Actual Production (in MT)",
      MAX(CASE WHEN q.question_id = '79dfe8f8-bf25-4914-8f00-4f4d747ae319' THEN a.answer_text END) AS "Mixer/Blender Running Hours (in hours)",
      MAX(CASE WHEN q.question_id = '10276bc7-957d-4f5d-b419-c82f05b4a679' THEN a.answer_text END) AS "Energy Consumption (in KWH)",
      MAX(CASE WHEN q.question_id = '6728727e-89a5-46c2-89a2-d80a60752999' THEN a.answer_text END) AS "No. of Product Changeover (in Nos)",
      MAX(CASE WHEN q.question_id = 'b9322d7a-394f-4366-b9a2-0d6059602a99' THEN a.answer_text END) AS "Changeover Time (in hours)",
      MAX(CASE WHEN q.question_id = '3e858bff-824e-4fa3-8fb6-87c573da64e1' THEN a.answer_text END) AS "Setup Time (in hours)",
      MAX(CASE WHEN q.question_id = '324a981d-058b-4a73-b15b-8ddc09cf14f3' THEN a.answer_text END) AS "Testing Time (in hours)",
      MAX(CASE WHEN q.question_id = 'ebef473f-814a-4a3b-bcad-3f51d6ba1dc5' THEN a.answer_text END) AS "Electical Breakdown Time (in hours)",
      MAX(CASE WHEN q.question_id = 'ebbc6d40-44e3-4b66-94d3-d742523885a6' THEN a.answer_text END) AS "Mechanical Breakdown Time (in hours)", 
      MAX(CASE WHEN q.question_id = '277bf217-919f-48a1-b8bb-97c7326cdf22' THEN a.answer_text END) AS "Unavailability of Raw Material (in hours)",
      MAX(CASE WHEN q.question_id = 'd82baa14-24fa-410b-a606-8b32fae5cf82' THEN a.answer_text END) AS "QC others (in hours)",
      MAX(CASE WHEN q.question_id = 'f8601c2a-277a-405f-a37c-fb3758aae88c' THEN a.answer_text END) AS "Other Stoppages (in hours)",
      MAX(CASE WHEN q.question_id = 'a346f69f-da32-4e70-a8b9-5f7c647f4b91' THEN a.answer_text END) AS "Loss Due To Absence of Worker (in MT)",
      MAX(CASE WHEN q.question_id = '435ec2a7-080f-49ee-9b18-7b49f8b999dd' THEN a.answer_text END) AS "Rejected Production (in MT)",
      MAX(CASE WHEN q.question_id = '4306598c-6ac1-4572-a226-ac3263555efc' THEN a.answer_text END) AS "Reprocess Material (in MT)"
    FROM swp.submissions s
    JOIN swp.users u1 ON s.requested_by = u1.user_id
    JOIN swp.users u2 ON s.authorizer = u2.user_id
    LEFT JOIN swp.questions q ON q.form_id = s.form_id
    LEFT JOIN swp.answers a ON a.submission_id = s.submission_id AND a.question_id = q.question_id
    WHERE s.form_id = 'dc5e9e79-87ca-44c8-b1f6-064b98227eb2'
    AND s.status = 'approved'
    GROUP BY s.submission_id, s.start_date, s.start_time, s.end_date, s.end_time, s.status, u1.first_name, u1.last_name, u2.first_name, u2.last_name
    ORDER BY s.start_date;
  `;

  const { rows } = await sourcePool.query(query);
  return rows;
};

// Function to fetch data for monthly table from source database
const fetchMonthlyData = async () => {
  const query = `
    SELECT
      s.submission_id,
      s.start_date + s.start_time AS "start_date",
      s.end_date + s.end_time AS "end_date",
      s.status,
      u1.first_name || ' ' || u1.last_name AS "requested_by",
      u2.first_name || ' ' || u2.last_name AS "authorizer",
      MAX(CASE WHEN q.question_id = 'b56951e9-4f4a-4f65-81d0-880cd7a6f5d3' THEN a.answer_text END) AS "MONTH",
      MAX(CASE WHEN q.question_id = '698308fe-9999-436d-bfea-49d124649fb8' THEN a.answer_text END) AS "5S SCORE",
      MAX(CASE WHEN q.question_id = '9c44f6ad-8d6f-409d-9052-de7a1e45d956' THEN a.answer_text END) AS "SOFI REPORTING",
      MAX(CASE WHEN q.question_id = 'b0c57b81-9b10-44e1-9b58-591c07001a8e' THEN a.answer_text END) AS "S7 REPORTING",
      MAX(CASE WHEN q.question_id = '47709abe-b375-4d4c-afaa-d6f7fbdee834' THEN a.answer_text END) AS "VFL REPORTING",
      MAX(CASE WHEN q.question_id = 'a312835f-1e4b-4d67-99d1-101a3c43e995' THEN a.answer_text END) AS "REJECTED MATERIAL (IN TON)(KQI 2)",
      MAX(CASE WHEN q.question_id = '041170df-805b-4dc7-b85f-4133e5542d9a' THEN a.answer_text END) AS "RECYCLED MATERIAL (IN TON)(KQI 4)",
      MAX(CASE WHEN q.question_id = '386cdec1-dcab-40f5-8d94-2da4f7eb79fd' THEN a.answer_text END) AS "EXPIRED MATERIAL IN WAREHOUSE (IN TON)(KQI 6)"
    FROM swp.submissions s
    JOIN swp.users u1 ON s.requested_by = u1.user_id
    JOIN swp.users u2 ON s.authorizer = u2.user_id
    LEFT JOIN swp.questions q ON q.form_id = s.form_id
    LEFT JOIN swp.answers a ON a.submission_id = s.submission_id AND a.question_id = q.question_id
    WHERE s.form_id = '897fea02-9719-45c3-8c08-3503934350d7'
      AND s.status = 'approved'
    GROUP BY s.submission_id, s.start_date, s.start_time, s.end_date, s.end_time, s.status, u1.first_name, u1.last_name, u2.first_name, u2.last_name
    ORDER BY s.start_date;
  `;

  const { rows } = await sourcePool.query(query);
  return rows;
};


const fetchBreakdownData = async () => {
  // const query = `
  //   SELECT
  //     s.submission_id,
  //     s.start_date + s.start_time AS "start_date",
  //     s.end_date + s.end_time AS "end_date",
  //     s.status,
  //     u1.first_name || ' ' || u1.last_name AS "requested_by",
  //     u2.first_name || ' ' || u2.last_name AS "authorizer",
  //     MAX(CASE WHEN q.question_id = 'c50fd0d6-d7b9-4ce2-b367-8c87a85d971c' THEN a.answer_text END) AS "Shift",
  //     MAX(CASE WHEN q.question_id = '9a5e4abd-e9b5-4083-9ba3-e997cc1e430c' THEN a.answer_text END) AS "Shift_Operator",
  //     MAX(CASE WHEN q.question_id = 'a1097f47-afc5-4763-8afd-2090fee75bab' THEN a.answer_text END) AS "stoppage",
  //     MAX(CASE WHEN q.question_id = '762e15d5-2f97-4f81-9bef-1fb63da24bb1' THEN a.answer_text END) AS "describe_the_stoppage"
  //   FROM swp.submissions s
  //   JOIN swp.users u1 ON s.requested_by = u1.user_id
  //   JOIN swp.users u2 ON s.authorizer = u2.user_id
  //   LEFT JOIN swp.questions q ON q.form_id = s.form_id
  //   LEFT JOIN swp.answers a ON a.submission_id = s.submission_id AND a.question_id = q.question_id
  //   WHERE s.form_id = 'e7aff97f-1b8b-487b-8553-cdeb46791770'
  //     AND s.status = 'approved'
  //   GROUP BY s.submission_id, s.start_date, s.start_time, s.end_date, s.end_time, s.status, u1.first_name, u1.last_name, u2.first_name, u2.last_name
  //   ORDER BY s.start_date;
  // `;
  const query = `
      SELECT
      s.submission_id,
      s.start_date + s.start_time AS "start_date",
      s.end_date + s.end_time AS "end_date",
      s.status,
      u1.first_name || ' ' || u1.last_name AS "requested_by",
      u2.first_name || ' ' || u2.last_name AS "authorizer",
      MAX(CASE WHEN q.question_id = '584ca2ff-bb09-42c7-af05-d68b66bb4317' THEN a.answer_text END) AS "Shift",
      MAX(CASE WHEN q.question_id = '29ac18ab-f01b-4097-98c9-fc0bc5b7cda9' THEN a.answer_text END) AS "Shift_Operator",
      MAX(CASE WHEN q.question_id = 'e7816494-ec3c-49a1-b917-a9270045c3c4' THEN a.answer_text END) AS "Stoppage",
      MAX(CASE WHEN q.question_id = '1f17c70f-6322-4ac4-a75b-ee38557ec4e8' THEN a.answer_text END) AS "Department",
      MAX(CASE WHEN q.question_id = '125babf0-ab98-45f7-b1fd-16444eee958d' THEN a.answer_text END) AS "Describe_the_stopaage",
      MAX(CASE WHEN q.question_id = '73e225fd-006e-48c1-a802-6dd40ce3c349' THEN a.answer_text END) AS "Duration"
    FROM swp.submissions s
    JOIN swp.users u1 ON s.requested_by = u1.user_id
    JOIN swp.users u2 ON s.authorizer = u2.user_id
    LEFT JOIN swp.questions q ON q.form_id = s.form_id
    LEFT JOIN swp.answers a ON a.submission_id = s.submission_id AND a.question_id = q.question_id
    WHERE s.form_id = 'd2feb706-6b79-4d07-b2ff-4d307974f973'
      AND s.status = 'approved'
    GROUP BY s.submission_id, s.start_date, s.start_time, s.end_date, s.end_time, s.status, u1.first_name, u1.last_name, u2.first_name, u2.last_name
    ORDER BY s.start_date;
  `;

  const { rows } = await sourcePool.query(query);
  return rows;
};

// Function to insert data into the destination database
const insertIntoDest = async (data) => {
  for (const row of data) {
    const checkQuery = `SELECT 1 FROM clayders.clayders_oee_new WHERE submission_id = $1`;
    const checkResult = await sourcePool.query(checkQuery, [row.submission_id]);

    if (checkResult.rowCount === 0) {
      // If not already present, insert the row
      const insertQuery = `
        INSERT INTO clayders.clayders_oee_new (
          submission_id, start_date, end_date, status, requested_by, authorizer, shift, shift_operator,
          headcount, "tonnes/manpower", "product/batch_selection", planned_production, actual_production, "mixer/blender running hour", energy_consumption,
          product_changeovers, changeover_time, setup_time, testing_time, elec_breakdown_time, mech_breakdown_time, raw_material_unavailability,
          qc_others, other_stoppages, loss_due_to_absence_of_worker, rejected_production, reprocess_material
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27
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
        row["Tonnes/Manpower"] || null,
        row["product/Batch Selection"] || null,
        row["Planned Production (in MT)"] || null,
        row["Actual Production (in MT)"] || null,
        row["Mixer/Blender Running Hours (in hours)"] || null,
        row["Energy Consumption (in KWH)"] || null,
        row["No. of Product Changeover (in Nos)"] || null,
        row["Changeover Time (in hours)"] || null,
        row["Setup Time (in hours)"] || null,
        row["Testing Time (in hours)"] || null,
        row["Electical Breakdown Time (in hours)"] || null,
        row["Mechanical Breakdown Time (in hours)"] || null,
        row["Unavailability of Raw Material (in hours)"] || null,
        row["QC others (in hours)"] || null,
        row["Other Stoppages (in hours)"] || null,
        row["Loss Due To Absence of Worker (in MT)"] || null,
        row["Rejected Production (in MT)"] || null,
        row["Reprocess Material (in MT)"] || null
      ];
      await sourcePool.query(insertQuery, values);
      console.log(`Data inserted for submission_id: ${row.submission_id}`);
    } else {
      //console.log("Data Avilable", checkResult.rowCount);
      //console.log(`Data already available for submission_id: ${row.submission_id}`);
    }
  }
};


// Function to insert data into the monthly table
const insertIntoMonthlyTable = async (data) => {
  for (const row of data) {
    const checkQuery = `SELECT 1 FROM clayders.clayders_monthly WHERE submission_id = $1`;
    const checkResult = await sourcePool.query(checkQuery, [row.submission_id]);

    if (checkResult.rowCount === 0) {
      // If not already present, insert the row
      const insertQuery = `
        INSERT INTO clayders.clayders_monthly (
          submission_id, start_date, end_date, status, requested_by, authorizer, month, "5s_score",
          sofi_reporting, s7_reporting, vlf_reporting, rejected_material, recycled_material, expired_material
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
        )
      `;
      const values = [
        row.submission_id || null,
        row.start_date || null,
        row.end_date || null,
        row.status || null,
        row.requested_by || null,
        row.authorizer || null,
        row.MONTH || null,
        row["5S SCORE"] || null,
        row["SOFI REPORTING"] || null,
        row["S7 REPORTING"] || null,
        row["VFL REPORTING"] || null,
        row["REJECTED MATERIAL (IN TON)(KQI 2)"] || null,
        row["RECYCLED MATERIAL (IN TON)(KQI 4)"] || null,
        row["EXPIRED MATERIAL IN WAREHOUSE (IN TON)(KQI 6)"] || null,
      ];
      await sourcePool.query(insertQuery, values);
      console.log(`Data inserted for submission_id: ${row.submission_id}`);
    } else {
      console.log(`Data already exists for submission_id: ${row.submission_id}`);
    }
  }
};

const insertIntoBreakdownTable = async (data) => {
  console.log(data);
  for (const row of data) {
    const checkQuery = `SELECT 1 FROM clayders.clayders_brekdowns WHERE submission_id = $1`;
    const checkResult = await sourcePool.query(checkQuery, [row.submission_id]);

    if (checkResult.rowCount === 0) {
      // If not already present, insert the row
      const insertQuery = `
        INSERT INTO clayders.clayders_brekdowns (
          submission_id, start_date, end_date, status, requested_by, authorizer, "Shift",
          "Shift_Operator", department, stoppage, describe_the_stoppage, duration
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
      `;
      const values = [
        row.submission_id || null,
        row.start_date || null,
        row.end_date || null,
        row.status || null,
        row.requested_by || null,
        row.authorizer || null,
        row["Shift"] || null,
        row["Shift_Operator"] || null,
        row["Department"] || null,
        row["Stoppage"] || null,
        row["Describe_the_stopaage"] || null,
        row["Duration"] || null
      ];
      await sourcePool.query(insertQuery, values);
      console.log(`Data inserted for submission_id: ${row.submission_id}`);
    } else {
      console.log(`Data already exists for submission_id: ${row.submission_id}`);
    }
  }
};


//Set up the cron job to run every 5 seconds
//const job = new cron.CronJob('*/10 * * * * *', async () => {
const job = new cron.CronJob('*/1 * * * *', async () => {
  console.log('Running cron job to fetch and insert data...');
  try {
    const data = await fetchSourceData();
    await insertIntoDest(data);

    const monthlyData = await fetchMonthlyData();
    await insertIntoMonthlyTable(monthlyData);


    const breakdownData = await fetchBreakdownData();
    await insertIntoBreakdownTable(breakdownData);

    console.log('Data inserted successfully.');
  } catch (error) {
    console.error('Error during cron job execution:', error);
  }
});

// Start the cron job
job.start();

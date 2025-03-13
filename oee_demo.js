// const { Client } = require('pg');

// const sourceDbConfig = {
//   user: 'senselive',
//   host: 'data.senselive.in',
//   database: 'senselive_db',
//   password: 'SenseLive@2025',
//   port: 5432, 
// };

// async function migrateData() {
//   const sourceClient = new Client(sourceDbConfig);

//   try {
//     await sourceClient.connect();
//     console.log("Connected to both source and destination databases");

//     const fetchQuery = `
//       SELECT id, timestamp, data
//       FROM ems_schema.device_data
//       where deviceUid = 'SLBANSALPLC' AND data::jsonb ? 'MC_STATUS'
//       ORDER BY timestamp ASC;
//     `;

//     const res = await sourceClient.query(fetchQuery);
//     const rows = res.rows;

//     for (let row of rows) {
//       console.log(`Processing record ID: ${row.id}`);
//       const insertQuery = `
//         INSERT INTO oee.device_data (deviceuid, "timestamp", data)
//         VALUES ($1, NOW(), $2);
//       `;

//       await sourceClient.query(insertQuery, ['OEETEST', row.data]);

//       console.log(`Inserted record ID: ${row.id} with current timestamp`);
//       await new Promise(resolve => setTimeout(resolve, 300000));
//     }

//     console.log("Migration completed!");

//   } catch (err) {
//     console.error("Error:", err);
//   } finally {
//     await sourceClient.end();
//     console.log("Disconnected from both databases");
//   }
// }

// migrateData();


/*------------------Version 2---------------------------*/
// const { Client } = require('pg');

// const sourceDbConfig = {
//   user: 'senselive',
//   host: 'data.senselive.in',
//   database: 'senselive_db',
//   password: 'SenseLive@2025',
//   port: 5432, 
// };

// async function migrateData() {
//   const sourceClient = new Client(sourceDbConfig);

//   try {
//     await sourceClient.connect();
//     console.log("Connected to the source database");

//     // Fetch all data sorted by timestamp
//     const fetchQuery = `
//       SELECT id, timestamp, data
//       FROM ems_schema.device_data
//       WHERE deviceUid = 'SLBANSALPLC' AND data::jsonb ? 'MC_STATUS'
//       ORDER BY timestamp ASC;
//     `;

//     const res = await sourceClient.query(fetchQuery);
//     const rows = res.rows;

//     if (rows.length === 0) {
//       console.log("No records found for migration.");
//       return;
//     }

//     console.log(`Fetched ${rows.length} records for migration`);

//     // Start inserting from 15th January 2025, 00:00:00
//     let newTimestamp = new Date('2025-02-18T00:00:00Z');

//     for (let i = 0; i < rows.length; i++) {
//       const row = rows[i];
//       console.log(`Processing record ID: ${row.id} at timestamp: ${newTimestamp}`);

//       // Insert the record with the new timestamp
//       const insertQuery = `
//         INSERT INTO oee.device_data (deviceuid, "timestamp", data)
//         VALUES ($1, $2, $3);
//       `;

//       await sourceClient.query(insertQuery, ['OEETEST', newTimestamp, row.data]);

//       console.log(`Inserted record ID: ${row.id} at timestamp: ${newTimestamp}`);

//       // Calculate time difference for the next entry
//       if (i < rows.length - 1) {
//         const currentTimestamp = new Date(row.timestamp);
//         const nextTimestamp = new Date(rows[i + 1].timestamp);

//         // Calculate the time difference
//         const timeDifference = nextTimestamp - currentTimestamp;

//         // Apply the same time difference to the new timestamp
//         newTimestamp = new Date(newTimestamp.getTime() + timeDifference);
//       }
//     }

//     console.log("Migration completed!");

//   } catch (err) {
//     console.error("Error:", err);
//   } finally {
//     await sourceClient.end();
//     console.log("Disconnected from the database");
//   }
// }

// migrateData();
/*-----------------------------------------Version 3--------------------------------*/
const { Client } = require('pg');

const sourceDbConfig = {
  user: 'senselive',
  host: 'data.senselive.in',
  database: 'senselive_db',
  password: 'SenseLive@2025',
  port: 5432, 
};

async function migrateData() {
  const sourceClient = new Client(sourceDbConfig);

  try {
    await sourceClient.connect();
    console.log("Connected to the source database");

    // Fetch all data sorted by timestamp
    const fetchQuery = `
      SELECT id, timestamp, data
      FROM ems_schema.device_data
      WHERE deviceUid = 'SLBANSALPLC' AND data::jsonb ? 'KWH'
      ORDER BY timestamp ASC;
    `;

    const res = await sourceClient.query(fetchQuery);
    const rows = res.rows;

    if (rows.length === 0) {
      console.log("No records found for migration.");
      return;
    }

    console.log(`Fetched ${rows.length} records for migration`);

    // Start inserting from 15th January 2025, 00:00:00
    let newTimestamp = new Date('2025-02-19T11:32:31.65Z');
    let batch = [];
    const batchSize = 1000;

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      // Store the values for batch insertion
      batch.push(`('OEETEST', '${newTimestamp.toISOString()}', '${JSON.stringify(row.data)}')`);

      // Calculate time difference for the next entry
      if (i < rows.length - 1) {
        const currentTimestamp = new Date(row.timestamp);
        const nextTimestamp = new Date(rows[i + 1].timestamp);
        const timeDifference = nextTimestamp - currentTimestamp;
        newTimestamp = new Date(newTimestamp.getTime() + timeDifference);
      }

      // Insert batch when it reaches the limit
      if (batch.length === batchSize || i === rows.length - 1) {
        const insertQuery = `
          INSERT INTO oee.device_data (deviceuid, "timestamp", data)
          VALUES ${batch.join(", ")};
        `;

        await sourceClient.query(insertQuery);
        console.log(`Inserted ${batch.length} records in batch`);
        batch = []; // Reset batch for next set
      }
    }

    console.log("Migration completed!");

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await sourceClient.end();
    console.log("Disconnected from the database");
  }
}

migrateData();

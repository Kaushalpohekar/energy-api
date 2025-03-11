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
    console.log("Connected to both source and destination databases");

    const fetchQuery = `
      SELECT id, timestamp, data
      FROM ems_schema.device_data
      where deviceUid = 'SLBANSALPLC' AND data::jsonb ? 'MC_STATUS'
      ORDER BY timestamp ASC;
    `;

    const res = await sourceClient.query(fetchQuery);
    const rows = res.rows;

    for (let row of rows) {
      console.log(`Processing record ID: ${row.id}`);
      const insertQuery = `
        INSERT INTO oee.device_data (deviceuid, "timestamp", data)
        VALUES ($1, NOW(), $2);
      `;

      await sourceClient.query(insertQuery, ['OEETEST', row.data]);

      console.log(`Inserted record ID: ${row.id} with current timestamp`);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    console.log("Migration completed!");

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await sourceClient.end();
    console.log("Disconnected from both databases");
  }
}

migrateData();

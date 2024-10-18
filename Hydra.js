const { Pool } = require('pg');

const pool = new Pool({
  user: 'senselive',
  host: 'pgsql.senselive.in',
  database: 'ems',
  password: 'SenseLive',
  port: 5432,
});

async function getDataByCustomDateStatus(req, res) {
  const deviceId = 'SL02202426-1';
  let counter = 0;
  
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const sql = `SELECT * FROM ems_schema.ems_actual_data WHERE device_uid = $1`;
    const results = await client.query(sql, [deviceId]);

    if (results.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'No data found for the given DeviceUID' });
    }

    const totalEntries = results.rowCount;
    console.log(`Total entries to insert: ${totalEntries}`);

    for (let i = 0; i < results.rows.length; i++) {
      const entry = results.rows[i];
      const data = {
        DeviceUID: 'SL02202426',
        1: entry.kwh,
        7: entry.kvarh,
        13: entry.kvah,
        15: entry.kw,
        17: entry.kva,
        19: entry.kvar,
        21: entry.voltage_n,
        23: entry.current,
        25: entry.pf,
        27: entry.freq,
      };

      const insertSql = `
        INSERT INTO ems_schema.device_data (deviceuid, "timestamp", "data")
        VALUES ($1, $2, $3)
      `;
      await client.query(insertSql, ['SL02202426', entry.date_time, JSON.stringify(data)]);
      counter++;

      const percentageComplete = ((counter / totalEntries) * 100).toFixed(2);
      console.log(`Progress: ${percentageComplete}% (${counter}/${totalEntries})`);
    }

    await client.query('COMMIT');
    console.log(`${counter} entries inserted successfully`);
    res.json({ message: `${counter} entries stored successfully` });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('An error occurred:', error);
    res.status(500).json({ message: 'Internal server error' });
  } finally {
    client.release();
  }
}

const fakeReq = {};
const fakeRes = {
  status: (code) => ({
    json: (message) => console.log(`Response [${code}]:`, message),
  }),
};

getDataByCustomDateStatus(fakeReq, fakeRes);

process.on('exit', () => {
  pool.end();
});

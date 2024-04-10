const mysql = require('mysql2');

const mysqlConfig = {
  host: '13.232.174.80',
  user: 'mysql',
  password: 'sense!123',
  database: 'orp',
  port: 3306,
};

const mysqlPool = mysql.createPool(mysqlConfig);

async function DeleteDataORP() {
  try {
    const query = 'DELETE FROM ORP_Meter WHERE orp IS NULL AND pump_1 IS NULL AND pump_2 IS NULL';
    const result = await mysqlPool.query(query);
    console.log('Rows deleted:', result.affectedRows);
  } catch (err) {
    console.error('Error deleting data from ORP_Meter:', err);
    throw err;
  }
}

process.on('exit', () => {
  mysqlPool.end();
});

DeleteDataORP(); // Call the function to execute the deletion

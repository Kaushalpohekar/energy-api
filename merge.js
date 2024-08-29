const mysql = require('mysql2/promise');

// Database connection
const dbConfig = {
  host: 'senso.senselive.in',
  user: 'mysql',
  password: 'sense!123',
  database: 'tms'
};

async function checkAndUpdateDevices() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Fetch all devices
    const [devices] = await connection.query('SELECT * FROM tms_devices');

    for (let device of devices) {
      const { DeviceUID } = device;

      // Fetch the latest entry for this device from actual_data
      const [latestEntry] = await connection.query(
        'SELECT * FROM actual_data WHERE DeviceUID = ? ORDER BY timestamp DESC LIMIT 1',
        [DeviceUID]
      );

      if (latestEntry.length > 0) {
        const latestTimestamp = new Date(latestEntry[0].TimeStamp);
        const currentTime = new Date();

        // Calculate the difference in minutes
        const differenceInMinutes = (currentTime - latestTimestamp) / (1000 * 60);

        if (differenceInMinutes > 30) {
          // Update the device as "offline" if no data in the last 30 minutes
          await connection.query(
            'UPDATE tms_devices SET status = ? WHERE DeviceUID = ?',
            ['offline', DeviceUID]
          );
          //console.log(`Device ${DeviceUID} marked as offline due to inactivity.`);
        } else {
          // Update the device as "online" if data is recent
          await connection.query(
            'UPDATE tms_devices SET status = ? WHERE DeviceUID = ?',
            ['online', DeviceUID]
          );
          //console.log(`Device ${DeviceUID} is online.`);
        }
      } else {
        // No data found, mark the device as "offline"
        await connection.query(
          'UPDATE tms_devices SET status = ? WHERE DeviceUID = ?',
          ['offline', DeviceUID]
        );
        //console.log(`Device ${DeviceUID} marked as offline due to no data.`);
      }
    }
  } catch (error) {
    console.error('Error checking and updating devices:', error);
  } finally {
    await connection.end();
  }
}

// Run the script every 5 seconds
setInterval(() => {
  // Get the current date and time
  const currentTime = new Date();
  
  // Format the date and time to "Aug 28, 2024 04:00 PM"
  const formattedTime = currentTime.toLocaleString('en-US', {
    month: 'short',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: true
  });

  console.log("Script is Running at the Time", formattedTime);
  checkAndUpdateDevices().catch(console.error);
}, 5000); // 5000 milliseconds = 5 seconds

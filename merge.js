const mysql = require('mysql2/promise');
const cron = require('node-cron');

// Database connection
const dbConfig = {
  // host: 'senso.senselive.in',
  // user: 'mysql',
  // password: 'sense!123',
  // database: 'tms'
  host: 'data.senselive.in',
  user: 'senselive',
  password: 'SenseLive@2025',
  database: 'tms',
  ssl: { rejectUnauthorized: false },
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

async function updateUserDeviceStats() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    // Fetch total devices and count online devices
    const [devices] = await connection.query('SELECT COUNT(*) AS total FROM tms_devices');
    const [onlineDevices] = await connection.query('SELECT COUNT(*) AS count FROM tms_devices WHERE status = "online"');
    const totalDevicesCount = devices[0].total;
    const onlineDevicesCount = onlineDevices[0].count;
    const offlineDevicesCount = totalDevicesCount - onlineDevicesCount;

    // Fetch total users and count online users
    const [users] = await connection.query('SELECT COUNT(*) AS total FROM tms_users');
    const [onlineUsers] = await connection.query('SELECT COUNT(*) AS count FROM tms_users WHERE is_online = 1');
    const totalUsersCount = users[0].total;
    const onlineUsersCount = onlineUsers[0].count;
    const offlineUsersCount = totalUsersCount - onlineUsersCount;

    // Count admin users
    const [adminUsers] = await connection.query('SELECT COUNT(*) AS count FROM tms_users WHERE UserType = "Admin"');
    const adminUsersCount = adminUsers[0].count;

    // Count standard users
    const [standardUsers] = await connection.query('SELECT COUNT(*) AS count FROM tms_users WHERE UserType = "Standard"');
    const standardUsersCount = standardUsers[0].count;

    const [totalOrganizations] = await connection.query('SELECT COUNT(DISTINCT CompanyEmail) AS count FROM tms_users');
    const totalOrganizationsCount = totalOrganizations[0].count;

    // Insert counts into tms_user_device_stats table
    await connection.query(
      'INSERT INTO tms_user_device_stats (active_users_count, total_users, active_devices_count, total_devices, inactive_users_count, deactive_devices_count, created_at, admin_users_count, standard_users_count, organizations) VALUES (?, ?, ?, ?, ?, ?, ?, ? ,? ,?)',
      [onlineUsersCount, totalUsersCount, onlineDevicesCount, totalDevicesCount, offlineUsersCount, offlineDevicesCount, new Date(), adminUsersCount, standardUsersCount, totalOrganizationsCount]
    );

    console.log('User and device stats updated successfully.');
  } catch (error) {
    console.error('Error updating user and device stats:', error);
  } finally {
    await connection.end();
  }
}

//Schedule the task to run every 5 seconds
cron.schedule('*/10 * * * * *', () => {
  // Get the current date and time
  const currentTime = new Date();

  // Format the date and time to "Aug 28, 2024 04:00 PM"
  const formattedTime = currentTime.toLocaleString('en-US', {
    month: 'short',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true
  });

  console.log("Script is Running at the Time for Device Updates Only", formattedTime);
  checkAndUpdateDevices().catch(console.error);
});

cron.schedule('*/1 * * * *', () => {
  const currentTime = new Date();

  // Format the date and time to "Aug 28, 2024 04:00 PM"
  const formattedTime = currentTime.toLocaleString('en-US', {
    month: 'short',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true
  });

  console.log("Script is Running at the Time for Users And Device Counts", formattedTime);
  updateUserDeviceStats().catch(console.error);
});
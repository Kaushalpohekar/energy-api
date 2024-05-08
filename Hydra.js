const axios = require('axios');
const mysql = require('mysql2/promise');
const cron = require('node-cron');

const pool = mysql.createPool({
  connectionLimit: 10,
  host: '13.127.102.12',
  user: 'mysql',
  password: 'sense!123',
  database: 'tms'
});

async function fetchLatestData(deviceId) {
  try {
    const [rows] = await pool.query(
      'SELECT DeviceUID, totalVolume FROM actual_data WHERE DeviceUID = ? ORDER BY TimeStamp DESC LIMIT 1',
      [deviceId]
    );
    return rows.length > 0 ? rows[0] : null;
  } catch (error) {
    console.error('Error fetching latest data: ', error);
    throw error;
  }
}

async function printData(deviceId) {
  try {
    const data = await fetchLatestData(deviceId);

    if (!data) {
      console.log(`No data found for device ${deviceId}`);
      return;
    }

    const timestamp = new Date().toLocaleString('en-US', {
      timeZone: 'Asia/Kolkata',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });

    let canNumber = '';
    if (data.DeviceUID === 'SL02202410') {
      canNumber = '033127923';
    } else if (data.DeviceUID === 'SL02202411') {
      canNumber = '033311359';
    }

    let reading = data.totalVolume;
    if (data.DeviceUID === 'SL02202410') {
      reading /= 100; // Divide by 100
    } else if (data.DeviceUID === 'SL02202411') {
      reading /= 1000; // Divide by 1000
    }

    const formattedData = {
      "can_number": canNumber,
      "mtr_number": data.DeviceUID,
      "reading": reading,
      "timestamp": timestamp.replace(/(\d+)\/(\d+)\/(\d+), (\d+):(\d+):(\d+)/, '$2-$1-$3 $4:$5:$6')
    };

    console.log('Latest data: ', formattedData);
    sendData(formattedData);
  } catch (error) {
    console.error('Error: ', error);
  }
}

// Function to print data for both devices with a delay
async function printDataWithDelay() {
  await printData('SL02202410');
  await new Promise(resolve => setTimeout(resolve, 5000)); // 5 seconds delay
  await printData('SL02202411');
}

//Schedule the cron job to run every 6 hours
cron.schedule('0 */6 * * *', async () => {
  console.log('Running data printing job...');
  await printDataWithDelay();
});

// cron.schedule('*/10 * * * * *', async () => {
//   console.log('Running data printing job...');
//   await printDataWithDelay();
// });

// Function to send data in the desired format
async function sendData(data) {
    const jsonData = {
      "cans_data": [data]
    };
  
    try {
//      https://mdm.hyderabadwater.gov.in/apis/api-testing.json
      const response = await axios.post('https://mdm.hyderabadwater.gov.in/apis/api-testing.json', jsonData, {
        headers: {
          'Content-Type': 'application/json',
        },
      });
  
      console.log('Data sent successfully:', response.data);
    } catch (error) {
      console.error('Error sending data:', error);
    }
}

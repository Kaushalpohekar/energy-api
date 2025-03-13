const mqtt = require('mqtt');
const { Client } = require('pg');
const os = require('os');

const broker = 'ws://dashboard.senselive.in:9001';

const pgConfig2 = {
  host: 'data.senselive.in',
  user: 'senselive',
  password: 'SenseLive@2025',
  database: 'senselive_db',
  port: 5432,
  ssl: { rejectUnauthorized: false },
  statement_timeout: 5000,
};

const pgClient2 = new Client(pgConfig2);

pgClient2.connect((err) => {
  if (err) {
    console.error('Error connecting to PostgreSQL database:', err.stack);
    return;
  }
  console.log('Connected to PostgreSQL database');
  startKeepAlive();
});

function startKeepAlive() {
  const keepAliveInterval = 60000; // 60 seconds
  setInterval(async () => {
    try {
      await pgClient2.query('SELECT 1');
    } catch (error) {
      try {
        await pgClient2.end();
      } catch {}
      
      let retries = 0;
      const reconnect = async () => {
        try {
          await pgClient2.connect();
        } catch {
          retries++;
          const delay = Math.min(60000, 5000 * retries);
          setTimeout(reconnect, delay);
        }
      };
      reconnect();
    }
  }, keepAliveInterval);
}


const options = {
  username: 'Sense2023',
  password: 'sense123',
  reconnectPeriod: 5000,
};

const mqttClient = mqtt.connect(broker, options);

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  mqttClient.subscribe('Sense/Live/#', (error) => {
    if (error) {
      console.error('Error subscribing to topics:', error);
    } else {
      console.log('Subscribed to all topics');
    }
  });
});

mqttClient.on('message', (topic, message) => {
  try {
    if (!message || message.length === 0) {
      //console.warn(`Received empty message on topic: ${topic}, skipping...`);
      return;
    }

    let data;
    try {
      data = JSON.parse(message.toString());
    } catch (error) {
      console.error(`Invalid JSON format received on topic: ${topic}, message: ${message}`);
      return;
    }

    if (data === null || typeof data !== 'object') {
      return;
    }

    const deviceUID = data.device_uid || data.DeviceUID || data.deviceid || data.deviceuid;
    const allowedDevices = ['SL02202413-1', 'SL02202413-2', 'SL02202413-3', 'SL02202413-4'];

    if (!deviceUID || !allowedDevices.includes(deviceUID)) {
      return;
    }

    //console.log('Data Collected For Device:', deviceUID);

    const localIpAddress = data.LocalIP || getLocalIp();
    const deviceStatus = data.status || data.Status || data.Error || data.error || 'online';

    if (data.totalVolume === 0 || data.Totalizer === 0) {
        //console.warn(`Skipping insertion for device ${deviceUID} because totalVolume is 0.`);
        return;
      }

    const insertQueryInEMS = `
      INSERT INTO tms.actual_data (
        deviceuid, timestamp, temperature, humidity, temperaturer, temperaturey, temperatureb, pressure, flowrate, totalvolume, ip_address, status
      ) VALUES (
        $1, NOW(), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
      )`;

    const insertValuesInEMS = [
      deviceUID,
      data.Temperature || data.temp || data.temperature || null,
      data.Humidity || data.hum || data.humidity || null,
      data.TemperatureR || data.tempr || data.temperaturer || null,
      data.TemperatureY || data.tempy || data.temperaturey || null,
      data.TemperatureB || data.tempb || data.temperatureb || null,
      data.Pressure || data.pressure || null,
      data.flowRate || data.FlowRate || data.Level || data.level || null,
      data.totalVolume || data.Totalizer || null,
      localIpAddress,
      deviceStatus
    ];

    pgClient2.query(insertQueryInEMS, insertValuesInEMS)
      .then(() => {
        console.log('Data inserted into PostgreSQL.', deviceUID);
      })
      .catch((error) => {
        console.error('Error inserting data into PostgreSQL:', error);
      });

  } catch (error) {
    console.error('Unexpected error processing message:', error);
  }
});

mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});

mqttClient.on('close', () => {
  console.warn('MQTT Connection Closed! Reconnecting...');
  setTimeout(() => {
    mqttClient.reconnect();
  }, 3000);
});

function getLocalIp() {
  const interfaces = os.networkInterfaces();
  for (let iface of Object.values(interfaces)) {
    for (let alias of iface) {
      if (alias.family === 'IPv4' && !alias.internal) {
        return alias.address;
      }
    }
  }
  return '0.0.0.0';
}

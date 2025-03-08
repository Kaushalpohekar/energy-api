const mqtt = require('mqtt');

// MQTT Broker details
const brokerUrl = 'mqtt://dashboard.senselive.in';
const port = 1883;
const username = 'Sense2023';
const password = 'sense123';
const topic = 'machine/data/WIRESIMULATION';

// Create an MQTT client instance
const client = mqtt.connect(brokerUrl, {
  port: port,
  username: username,
  password: password
});

// Variables to track incremental values for KVAH, KVARH, and KWH
let lastKVAH = 0.0;
let lastKVARH = 0.0;
let lastKWH = 0.0;

// State tracking variables
let lastMcStatusChangeTime = Date.now();
let currentMcStatus = Math.random() < 0.7 ? 1 : 0; // Initial machine status
let activeBreakdown = null;
let breakdownStartTime = null;
let blockChangeTime = Date.now();
let activeBlock = Math.floor(Math.random() * 7) + 1; // Random initial active block

// Function to generate machine test data
let lastMonthProduction = 0; // Store last month's production for condition 4
let emergencyTimer = 0; // Track emergency timing for condition 7
let lastBobbinChangeTime = Date.now(); // Track time for bobbin change condition
let thisMonthProduction = 0; // Store this month's production
let lastProductionUpdateTime = Date.now(); // Store the last production update time
let actualProduction = 0;
let previousActSpeed = 25.0;

function generateMachineTestData() {
  const targetSpeed = 30.0; // Condition 10: Constant target speed
  const now = Date.now();

  // Machine status (1 running, 0 stopped)
  let mcStatus;
  let actSpeed = 0;
  const activeDuration = 12 * 60 * 60 * 1000; // 12 hours in milliseconds

  previousActSpeed = actSpeed;

  if (now - blockChangeTime > 10 * 60 * 1000) { // Trigger block change every 10 minutes
    activeBlock = Math.floor(Math.random() * 7) + 1; // Select a random block
    blockChangeTime = now; // Update the last block change time
  
    // Schedule block deactivation after 2 minutes
    setTimeout(() => {
      activeBlock = null; // Deactivate the block after 2 minutes
    }, 2 * 60 * 1000); // 2 minutes in milliseconds
  }

  const blocks = Array.from({ length: 7 }, (_, i) => i + 1).reduce((acc, num) => {
    acc[`Block${num} Door Ground`] = actSpeed > 0 && num === activeBlock;
    acc[`Block${num} JOG FWD`] = false;
    acc[`Block${num} JOG REV`] = false;
    acc[`Block${num} Single/Multi`] = false;
    acc[`Block${num} WBS`] = false;
    acc[`Block${num} Wire Size`] = +(2.5 + Math.random() * 2.5).toFixed(2); // Random between 2.5mm and 5mm
    return acc;
  }, {});

  // Handle breakdowns: less frequent and last for 3-4 minutes
  const e_dt_keys = [
    "E_DT_BRAKE_SV", "E_DT_DANCER_ISSUE", "E_DT_DRIVE_FAULT",
    "E_DT_ELECRTICAL_MAINTNANCE", "E_DT_LIMIT_SWITCH", "E_DT_MOTOR_FAULT",
    "E_DT_SENSOR_PROBLEM"
  ];
  let e_dt_status = e_dt_keys.reduce((acc, key) => {
    acc[key] = 0;
    return acc;
  }, {});

  // Ensure MC_STATUS is only 0 when a breakdown is active
  if (activeBreakdown && now - breakdownStartTime < 3 * 60 * 1000 + Math.random() * 9 * 60 * 1000) {
    e_dt_status[activeBreakdown] = 1;
    actSpeed = 0; // Stop machine on breakdown
    mcStatus = 0; // Ensure machine stops only during breakdown
  } else {
    activeBreakdown = null;
    mcStatus = 1;
      if (now - lastBobbinChangeTime < activeDuration) {
        const speedChange = (Math.random() * 10) - 5;
        const newSpeed = previousActSpeed + speedChange;
  
        actSpeed = Math.max(0.1, Math.min(targetSpeed, newSpeed));
  
        actSpeed = +actSpeed.toFixed(2);
      }
    if (Math.random() < 0.01) { // 1% chance of breakdown
        activeBreakdown = e_dt_keys[Math.floor(Math.random() * e_dt_keys.length)];
        breakdownStartTime = now;
    }
  }

  // Condition 3: Bobbin Former Change every 30 to 60 minutes
  let P_DT_BOBIN_FORMER_CHANGE = 0;
  if (now - lastBobbinChangeTime > 30 * 60 * 1000 && mcStatus == 1) {
    P_DT_BOBIN_FORMER_CHANGE = 1;
    lastBobbinChangeTime = now;
  }

  // Handle production values
  const previousMonthProduction = lastMonthProduction;
  const isNewMonth = new Date().getDate() === 1;
  if (isNewMonth) {
    thisMonthProduction = 0;
  }

  if (mcStatus === 1 && actSpeed > 0) {
    thisMonthProduction += 5 * actSpeed;
  } else if (!isNewMonth && now - lastProductionUpdateTime > 60000) {
    lastProductionUpdateTime = now;
    thisMonthProduction += 5 * actSpeed;
  }

  const speedIncrease = actSpeed < targetSpeed;
  const speedDecrease = actSpeed > 0 && actSpeed >= targetSpeed;

  // Condition 7: Emergency every 2-3 hours for 15 min
  const emergency = emergencyTimer > 0;
  if (emergency) {
    emergencyTimer -= 1;
  } else if (Math.random() < 0.005) {
    emergencyTimer = 15; // 15 min emergency
  }

  // Condition 8: Line Speed mirrors Act Speed
  const lineSpeed = actSpeed;

  // Motor data
  const motorData = Array.from({ length: 7 }).reduce((acc, _, i) => {
    acc[`Motor ${i + 1} Current`] = +(Math.random() * 0.1).toFixed(6); // Random current between 0 and 0.1
    acc[`Motor ${i + 1} HZ`] = +(Math.random() * 100).toFixed(2); // Random HZ between 0 and 100
    return acc;
  }, {});

  // O_DT fields
  const o_dt = Array.from({ length: 16 }).reduce((acc, _, i) => {
    acc[`O_DT_${i + 10}`] = 0;
    return acc;
  }, {});
  o_dt["O_DT_AIR_UNAVAILABLE"] = 0;
  o_dt["O_DT_NO_PRODUCTION_PLAN"] = 0;
  o_dt["O_DT_OPERATOR_UNAVAILABLE"] = 0;
  o_dt["O_DT_POWER_CUT"] = 0;
  o_dt["O_DT_WATER_UNAVAILABLE"] = 0;

  return {
    "Act Speed": actSpeed,
    "ACT_COLD_DIA": +(Math.random() * 3).toFixed(2),
    "Block 2 Wire Size": blocks["Block2 Wire Size"],
    "Block 6 Wire Size": blocks["Block6 Wire Size"],
    ...blocks,
    "Break Release": false,
    "DIA SHEDULE": "5.5mm_To_3.10mm",
    ...e_dt_status,
    "Emergency": emergency,
    "Fault Reset": true,
    "Inlet Wire Size": 8.0,
    "Length Reset": false,
    "LINE_SPEED": lineSpeed,
    "MC_STATUS": mcStatus,
    ...motorData,
    ...o_dt,
    "P_DT_BOBIN_FORMER_CHANGE": P_DT_BOBIN_FORMER_CHANGE,
    "previous Month Production": previousMonthProduction,
    "Previous Shift Hours": 0.0,
    "Previous Shift Min": 0.0,
    "Previous Shift Production": 1461.86,
    "Quick Stop": false,
    "Running Shift Hours": 0.0,
    "Running Shift Min": 0.0,
    "Running Shift Production": 1593.22,
    "Speed decrease": speedDecrease,
    "Speed Increase": speedIncrease,
    "Start": mcStatus === 1,
    "Stop": mcStatus === 0,
    "Target Speed": targetSpeed,
    "This Month Production": actualProduction + thisMonthProduction,
    "TOTAL_LENGTH": 0.0
  };
}

function generatePowerSystemData() {
  const PF = +(Math.random() * 0.3 + 0.7).toFixed(2);
  const current_I = +(Math.random() * 0.2 + 0.05).toFixed(2);
  const KVA = +(Math.random() * 10).toFixed(2);
  const KVAR = +(Math.random() * 10).toFixed(2);
  const KW = +(Math.random() * 0.2 + 0.05).toFixed(2);

  lastKVAH += +KVA;
  lastKVARH += +KVAR;
  lastKWH += +KW;

  const KWH = +(lastKWH).toFixed(2);
  const KVAH = +(lastKVAH).toFixed(2);
  const KVARH = +(lastKVARH).toFixed(2);
  const voltage_LL = +(Math.random() * 50 + 380).toFixed(8);
  const voltage_LN = +(Math.random() * 20 + 220).toFixed(8);

  return {
    current_I,
    KVA,
    KVAH,
    KVAR,
    KVARH,
    KW,
    KWH,
    PF,
    voltage_LL,
    voltage_LN
  };
}

client.on('connect', () => {
  console.log('Connected to MQTT broker');

  // Publish data every 2.5 seconds (2500 ms), alternating between machine data and power system data
  let toggle = true;

  setInterval(() => {
    let data;

    // Check if it's the first day of the month
    const currentDate = new Date();
    const isFirstDayOfMonth = currentDate.getDate() === 1;
    const isFirstHourOfDay = currentDate.getHours() === 0 && currentDate.getMinutes() === 0;

    if (isFirstDayOfMonth && isFirstHourOfDay) {
      actualPorduction = 0;
    }

    if (toggle) {
      data = generateMachineTestData();
      actualPorduction = data['This Month Production'];
    } else {
      data = generatePowerSystemData();
    }
    toggle = !toggle;

    // Send data as a JSON string to the MQTT broker
    client.publish(topic, JSON.stringify(data), { qos: 1 }, (err) => {
      if (err) {
        console.error('Failed to publish data:', err);
      } else {
        console.log('Data successfully published');
      }
    });
  }, 10 * 1000); // Publish every 2.5 seconds
});

// Handle errors
client.on('error', (err) => {
  console.error('Error connecting to MQTT broker:', err);
});
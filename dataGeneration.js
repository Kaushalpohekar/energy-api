const mqtt = require('mqtt');

// MQTT Broker details
const brokerUrl = 'mqtt://dashboard.senselive.in';
const port = 1883;
const username = 'Sense2023';
const password = 'sense123';
const topic = 'machine/data/OEETEST';

// Create an MQTT client instance
const client = mqtt.connect(brokerUrl, {
  port: port,
  username: username,
  password: password
});

// Function to generate machine test data (same as you provided before)
let lastMonthProduction = 0; // Store last month's production for condition 4
let emergencyTimer = 0; // Track emergency timing for condition 7
let lastBobbinChangeTime = Date.now(); // Track time for bobbin change condition
let lastE_DT_Status = ""; // Track last E_DT set to 1
let thisMonthProduction = 0; // Store this month's production
let lastProductionUpdateTime = Date.now(); // Store the last production update time
let actualPorduction = 0;

function generateMachineTestData() {
  const targetSpeed = 30.0; // Condition 10: Constant target speed
  const mcStatus = Math.random() < 0.7 ? 1 : 0; // Machine status (1 running, 0 stopped)

  // Condition 1: Act Speed depends on MC Status and cannot exceed target speed
  let actSpeed = 0;
  let activeTime = 0;
  const activeDuration = 12 * 60 * 60 * 1000; // 12 hours in milliseconds

  // If the machine is active, ensure it stays active for at least 12 hours of the day
  if (mcStatus === 1) {
    // If it's within the 12-hour active period
    const currentTime = Date.now();
    if (currentTime - lastBobbinChangeTime < activeDuration) {
      actSpeed = +(Math.random() * targetSpeed).toFixed(2);
      activeTime = currentTime;
    } else {
      // Reset if the 12 hours have passed
      actSpeed = 0;
    }
  }

  // Block handling: no block should halt the process
  const activeBlock = Math.floor(Math.random() * 7) + 1;
  const blocks = Array.from({ length: 7 }, (_, i) => i + 1).reduce((acc, num) => {
    acc[`Block${num} Door Ground`] = actSpeed > 0 && num === activeBlock;
    acc[`Block${num} JOG FWD`] = false;
    acc[`Block${num} JOG REV`] = false;
    acc[`Block${num} Single/Multi`] = false;
    acc[`Block${num} WBS`] = false;
    acc[`Block${num} Wire Size`] = +(2.5 + Math.random() * 2.5).toFixed(2); // Random between 2.5mm and 5mm
    return acc;
  }, {});

  // Condition 3: Bobbin Former Change every 30 to 60 minutes
  const currentTime = Date.now();
  let P_DT_BOBIN_FORMER_CHANGE = 0;
  if (currentTime - lastBobbinChangeTime > 30 * 60 * 1000) {
    P_DT_BOBIN_FORMER_CHANGE = 1;
    lastBobbinChangeTime = currentTime;
  }

  // Condition 4 & 5: Previous and This Month Production
  const previousMonthProduction = lastMonthProduction;

  // Ensure production doesn't reset unless it's a new month
  const isNewMonth = new Date().getDate() === 1;
  if (isNewMonth) {
    thisMonthProduction = 0;
  }

  // Condition for increasing This Month Production by 5 * Act Speed
  if (mcStatus === 1 && actSpeed > 0) {
    thisMonthProduction += 5 * actSpeed;
  } else if (!isNewMonth && Date.now() - lastProductionUpdateTime > 60000) {
    // Production update happens once every minute
    lastProductionUpdateTime = Date.now();
    thisMonthProduction += 5 * actSpeed; // Increase production by 5 * Act Speed every minute
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

  // Condition 9: E_DT Error Logic
  const e_dt_keys = [
    "E_DT_BRAKE_SV", "E_DT_DANCER_ISSUE", "E_DT_DRIVE_FAULT",
    "E_DT_ELECRTICAL_MAINTNANCE", "E_DT_LIMIT_SWITCH", "E_DT_MOTOR_FAULT",
    "E_DT_SENSOR_PROBLEM"
  ];
  let e_dt_status = e_dt_keys.reduce((acc, key) => {
    acc[key] = 0;
    return acc;
  }, {});
  if (mcStatus === 1 && Math.random() < 0.05) {
    const randomE_DT = e_dt_keys[Math.floor(Math.random() * e_dt_keys.length)];
    e_dt_status[randomE_DT] = 1;
    actSpeed = 0; // Machine stops when an error is active
  }

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

  // M_DT fields (similar to O_DT, added for completeness)
  const m_dt = Array.from({ length: 16 }).reduce((acc, _, i) => {
    acc[`M_DT_${i + 10}`] = 0;
    return acc;
  }, {});
  m_dt["M_DT_DRUM_ISSUE"] = 0;
  m_dt["M_DT_GEAR_BEARING"] = 0;
  m_dt["M_DT_GEAR_MAINTNANCE"] = 0;
  m_dt["M_DT_MECHANICAL_MAINTNANACE"] = 0;
  m_dt["M_DT_OIL_SEAL_LEAKAGE"] = 0;

  return {
    "Act Speed": actSpeed,
    "ACT_COLD_DIA": +(Math.random() * 3).toFixed(2), // Random diameter
    "Block 2 Wire Size": +(2.5 + Math.random() * 2.5).toFixed(2),
    "Block 6 Wire Size": +(2.5 + Math.random() * 2.5).toFixed(2),
    "Block1 Door Ground": blocks["Block1 Door Ground"],
    "Block1 JOG FWD": blocks["Block1 JOG FWD"],
    "Block1 JOG REV": blocks["Block1 JOG REV"],
    "Block1 Single/Multi": blocks["Block1 Single/Multi"],
    "Block1 Single/Multi HMI": blocks["Block1 Single/Multi"],
    "Block1 WBS": blocks["Block1 WBS"],
    "Block1 Wire Size": blocks["Block1 Wire Size"],
    "Block2 Door Ground": blocks["Block2 Door Ground"],
    "Block2 JOG FWD": blocks["Block2 JOG FWD"],
    "Block2 JOG REV": blocks["Block2 JOG REV"],
    "Block2 Single/Multi": blocks["Block2 Single/Multi"],
    "Block2 WBS": blocks["Block2 WBS"],
    "Block3 Door Ground": blocks["Block3 Door Ground"],
    "Block3 JOG FWD": blocks["Block3 JOG FWD"],
    "Block3 JOG REV": blocks["Block3 JOG REV"],
    "Block3 Single/Multi": blocks["Block3 Single/Multi"],
    "Block3 WBS": blocks["Block3 WBS"],
    "Block3 Wire Size": blocks["Block3 Wire Size"],
    "Block4 Door Ground": blocks["Block4 Door Ground"],
    "Block4 JOG FWD": blocks["Block4 JOG FWD"],
    "Block4 JOG REV": blocks["Block4 JOG REV"],
    "Block4 Single/Multi": blocks["Block4 Single/Multi"],
    "Block4 WBS": blocks["Block4 WBS"],
    "Block4 Wire Size": blocks["Block4 Wire Size"],
    "Block5 Door Ground": blocks["Block5 Door Ground"],
    "Block5 JOG FWD": blocks["Block5 JOG FWD"],
    "Block5 JOG REV": blocks["Block5 JOG REV"],
    "Block5 Single/Multi": blocks["Block5 Single/Multi"],
    "Block5 WBS": blocks["Block5 WBS"],
    "Block5 Wire Size": blocks["Block5 Wire Size"],
    "Block6 Door Ground": blocks["Block6 Door Ground"],
    "Block6 JOG FWD": blocks["Block6 JOG FWD"],
    "Block6 JOG REV": blocks["Block6 JOG REV"],
    "Block6 Single/Multi": blocks["Block6 Single/Multi"],
    "Block6 WBS": blocks["Block6 WBS"],
    "Block7 Door Ground": blocks["Block7 Door Ground"],
    "Block7 JOG FWD": blocks["Block7 JOG FWD"],
    "Block7 JOG REV": blocks["Block7 JOG REV"],
    "Block7 Single/Multi": blocks["Block7 Single/Multi"],
    "Block7 WBS": blocks["Block7 WBS"],
    "Block7 Wire Size": blocks["Block7 Wire Size"],
    "Break Release": false,
    "DIA SHEDULE": "5.5mm_To_3.10mm",
    "E_DT_10": e_dt_status["E_DT_BRAKE_SV"],
    "E_DT_11": e_dt_status["E_DT_DANCER_ISSUE"],
    "E_DT_12": e_dt_status["E_DT_DRIVE_FAULT"],
    "E_DT_13": e_dt_status["E_DT_ELECRTICAL_MAINTNANCE"],
    "E_DT_14": e_dt_status["E_DT_LIMIT_SWITCH"],
    "E_DT_15": e_dt_status["E_DT_MOTOR_FAULT"],
    "E_DT_16": e_dt_status["E_DT_SENSOR_PROBLEM"],
    "Emergency": emergency,
    "Fault Reset": true,
    "Inlet Wire Size": 8.0,
    "Length Reset": false,
    "LINE_SPEED": lineSpeed,
    "MC_STATUS": mcStatus,
    "Motor 1 Current": motorData["Motor 1 Current"],
    "Motor 1 HZ": motorData["Motor 1 HZ"],
    "Motor 2 Current": motorData["Motor 2 Current"],
    "Motor 2 HZ": motorData["Motor 2 HZ"],
    "Motor 3 Current": motorData["Motor 3 Current"],
    "Motor 3 HZ": motorData["Motor 3 HZ"],
    "Motor 4 Current": motorData["Motor 4 Current"],
    "Motor 4 HZ": motorData["Motor 4 HZ"],
    "Motor 5 Current": motorData["Motor 5 Current"],
    "Motor 5 HZ": motorData["Motor 5 HZ"],
    "Motor 6 Current": motorData["Motor 6 Current"],
    "Motor 6 HZ": motorData["Motor 6 HZ"],
    "Motor 7 Current": motorData["Motor 7 Current"],
    "Motor 7 HZ": motorData["Motor 7 HZ"],
    "O_DT_10": o_dt["O_DT_10"],
    "O_DT_11": o_dt["O_DT_11"],
    "O_DT_12": o_dt["O_DT_12"],
    "O_DT_13": o_dt["O_DT_13"],
    "O_DT_14": o_dt["O_DT_14"],
    "O_DT_15": o_dt["O_DT_15"],
    "O_DT_16": o_dt["O_DT_16"],
    "O_DT_6": o_dt["O_DT_6"],
    "O_DT_7": o_dt["O_DT_7"],
    "O_DT_8": o_dt["O_DT_8"],
    "O_DT_9": o_dt["O_DT_9"],
    "O_DT_AIR_UNAVAILABLE": o_dt["O_DT_AIR_UNAVAILABLE"],
    "O_DT_NO_PRODUCTION_PLAN": o_dt["O_DT_NO_PRODUCTION_PLAN"],
    "O_DT_OPERATOR_UNAVAILABLE": o_dt["O_DT_OPERATOR_UNAVAILABLE"],
    "O_DT_POWER_CUT": o_dt["O_DT_POWER_CUT"],
    "O_DT_WATER_UNAVAILABLE": o_dt["O_DT_WATER_UNAVAILABLE"],
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
    "This Month Production": actualPorduction + thisMonthProduction,
    "TOTAL_LENGTH": 0.0
  };
}

console.log(generateMachineTestData());




client.on('connect', () => {
  console.log('Connected to MQTT broker');

  // Publish data to the topic every 5 seconds
  setInterval(() => {
    const machineData = generateMachineTestData();
    actualPorduction = machineData['This Month Production'];

    // Send data as a JSON string to the MQTT broker
    client.publish(topic, JSON.stringify(machineData), { qos: 1 }, (err) => {
      if (err) {
        console.error('Failed to publish data:', err);
      } else {
        console.log('Data successfully published');
      }
    });
  }, 5000); // Publish every 5 seconds
});

// Handle errors
client.on('error', (err) => {
  console.error('Error connecting to MQTT broker:', err);
});
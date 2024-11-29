const mqtt = require('mqtt');

const options = {
    host: 'dashboard.senselive.in',
    port: 1883,
    username: 'Sense2023',
    password: 'sense123',
};

const client = mqtt.connect(options);

client.on('connect', () => {
    console.log('Connected to MQTT broker');
});

client.on('error', (err) => {
    console.error('MQTT Error:', err.message);
});

const topic = 'machine/data/OEETEST';


const generateMachineData = () => {
    let machineData = {
        MC_STATUS: 0,
        LINE_SPEED: 0,
        Act_Speed: 0.5,
        Target_Speed: 30.0,
        Production: 0,
        Previous_Production: 463315.0625,
        Emergency: false,
        Fault_Reset: false,
        Motor_Fault: 0,
        Electrical_Maintenance: 0,
        Mechanical_Maintenance: 0,
        Sensor_Fault: 0,
        Drive_Fault: 0,
        Limit_Switch: 0,
        Die_Unavailable: 0,
        Wire_Brakage: 0,
        Coil_Unavailable: 0,
        Die_Change: 0,
        Wire_Size: 4.8,
        Inlet_Wire_Size: 8.0,
        Shift_Hours: 0,
        Shift_Production: 0,
        Previous_Shift_Production: 1461.862548828125,
        DIA_SCHEDULE: "5.5mm_To_3.10mm",
        Length_Reset: false,
        Quick_Stop: false,
        Previous_Month_Production: 463315.0625,
        Previous_Shift_Hours: 0.0,
        Previous_Shift_Min: 0.0,
        This_Month_Production: 3055.08056640625,
        TOTAL_LENGTH: 0.0,
        E_DT_10: 0,
        E_DT_11: 0,
        E_DT_12: 0,
        E_DT_13: 0,
        E_DT_14: 0,
        E_DT_15: 0,
        E_DT_16: 0,
        E_DT_8: 0,
        E_DT_9: 0,
        E_DT_BRAKE_SV: 0,
        E_DT_DANCER_ISSUE: 0,
        E_DT_DRIVE_FAULT: 0,
        E_DT_ELECRTICAL_MAINTNANCE: 0,
        E_DT_LIMIT_SWITCH: 0,
        E_DT_MOTOR_FAULT: 0,
        E_DT_SENSOR_PROBLEM: 0,
        M_DT_10: 0,
        M_DT_11: 0,
        M_DT_12: 0,
        M_DT_13: 0,
        M_DT_14: 0,
        M_DT_15: 0,
        M_DT_16: 0,
        M_DT_6: 0,
        M_DT_7: 0,
        M_DT_8: 0,
        M_DT_9: 0,
        M_DT_DRUM_ISSUE: 0,
        M_DT_GEAR_BEARING: 0,
        M_DT_GEAR_MAINTENANCE: 0,
        M_DT_MECHANICAL_MAINTNANACE: 0,
        M_DT_OIL_SEAL_LEAKAGE: 0,
        Motor1_Current: 0.020294189453125,
        Motor1_HZ: 0.0,
        Motor2_Current: 0.021026611328125,
        Motor2_HZ: 0.0,
        Motor3_Current: 0.02523193508386612,
        Motor3_HZ: 0.0,
        Motor4_Current: 0.020294189453125,
        Motor4_HZ: 0.0,
        Motor5_Current: 0.0162353515625,
        Motor5_HZ: 0.0,
        Motor6_Current: 0.020294189453125,
        Motor6_HZ: 0.0,
        Motor7_Current: 0.021026611328125,
        Motor7_HZ: 0.0,
        O_DT_10: 0,
        O_DT_11: 0,
        O_DT_12: 0,
        O_DT_13: 0,
        O_DT_14: 0,
        O_DT_15: 0,
        O_DT_16: 0,
        O_DT_6: 0,
        O_DT_7: 0,
        O_DT_8: 0,
        O_DT_9: 0,
        O_DT_AIR_UNAVAILABLE: 0,
        O_DT_NO_PRODUCTION_PLAN: 0,
        O_DT_OPERATOR_UNAVAILABLE: 0,
        O_DT_POWER_CUT: 0,
        O_DT_WATER_UNAVAILABLE: 0,
        OPRATOR_NO: "RL01",
        P_DT_13: 0,
        P_DT_14: 0,
        P_DT_15: 0,
        P_DT_16: 0,
        P_DT_BOBIN_FORMER_CHANGE: 0,
        P_DT_BOBIN_FORMER_UNAVAILABLE: 0,
        P_DT_COIL_CHANGE: 0,
        P_DT_COIL_UNAVAILABLE: 0,
        P_DT_DIE_CHANGE: 0,
        P_DT_DIE_UNAVAILABLE: 0,
        P_DT_WIRE_BRAKAGE: 0
    };

    function startMachine() {
        if (machineData.MC_STATUS === 0 && !machineData.Emergency) {
            machineData.MC_STATUS = 1;
            machineData.Emergency = false;
            machineData.Fault_Reset = true;
        }
    }

    function simulateBreakdown() {
        if (Math.random() < 0.1) {
            const faults = [
                "Motor_Fault", "Electrical_Maintenance", "Mechanical_Maintenance", "Sensor_Fault", "Drive_Fault",
                "Limit_Switch", "Die_Unavailable", "Wire_Brakage", "Coil_Unavailable", "Die_Change"
            ];
            const randomFault = faults[Math.floor(Math.random() * faults.length)];
            machineData[randomFault] = 1;
            machineData.MC_STATUS = 0;
            machineData.LINE_SPEED = 0;
            machineData.Production = 0;
            machineData.Act_Speed = 0.5;
        }
    }

    function updateMachineData() {
        if (machineData.MC_STATUS === 1) {
            if (machineData.LINE_SPEED < machineData.Target_Speed) {
                machineData.LINE_SPEED += 1;
                machineData.Act_Speed = machineData.LINE_SPEED / 100;
            }
            machineData.Production += (machineData.LINE_SPEED / 100) * 10;
            machineData.Shift_Production = machineData.Production - machineData.Previous_Production;
        }

        simulateBreakdown();

        if (machineData.MC_STATUS === 1 && machineData.Production > 0) {
            machineData.Shift_Production = machineData.Production - machineData.Previous_Production;
        }

        //   console.log(JSON.stringify(machineData));
        client.publish(topic, JSON.stringify(machineData), { qos: 1 }, (err) => {
            if (err) {
                console.log('Error sending data to MQTT:', err);
            } else {
                console.log('Data sent to MQTT broker');
            }
        });

        machineData.Previous_Production = machineData.Production;
    }

    startMachine();

    setInterval(updateMachineData, 5000);
};

generateMachineData();

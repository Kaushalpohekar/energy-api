const { Pool } = require('pg');
const ejs = require('ejs');
const fs = require('fs');
const path = require('path');
const nodemailer = require('nodemailer');
const cron = require('node-cron');

const sourcePool = new Pool({
    host: 'data.senselive.in',
    user: 'senselive',
    password: 'SenseLive@2025',
    database: 'senselive_db',
    port: 5432,
    ssl: { rejectUnauthorized: false },
});

const transporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 465,
    secure: true,
    auth: {
        user: 'donotreplysenselive@gmail.com',
        pass: 'xgcklimtlbswtzfq',
    },
});

const deviceNameMap = {
    "SL_LoRa_00001_01": { name: "MIDC Main Line", type: "Flow Sensor" },
    "SL_LoRa_00002_01": { name: "Admin Gardening", type: "Flow Sensor" },
    "SL_LoRa_00002_02": { name: "Admin WashRoom", type: "Flow Sensor" },
    "SL_LoRa_00003_01": { name: "Raw Water Furnance Tank", type: "Flow Sensor" },
    "SL_LoRa_00003_02": { name: "Raw Furnance 1 & 2", type: "Flow Sensor" },
    "SL_LoRa_00003_03": { name: "Raw Water Next I", type: "Flow Sensor" },
    "SL_LoRa_00003_04": { name: "Soft Water 1 Tank", type: "Level Sensor" },
    "SL_LoRa_00003_05": { name: "Soft Water 2 Tank", type: "Level Sensor" },
    "SL_LoRa_00003_06": { name: "Soft Water 3 Tank", type: "Level Sensor" },
    "SL_LoRa_00003_07": { name: "Furnance Overall", type: "Flow Sensor" },
    "SL_LoRa_00003_08": { name: "Emergency Tank ", type: "Level Sensor" },
    "SL_LoRa_00003_09": { name: "Operator Utility", type: "Flow Sensor" },
    "SL_LoRa_00004_01": { name: "Next I Scrubber", type: "Flow Sensor" },
    "SL_LoRa_00005_01": { name: "Mold Cooling", type: "Flow Sensor" },
    "SL_LoRa_00006_01": { name: "Next I Gardening", type: "Flow Sensor" },
    "SL_LoRa_00006_02": { name: "Emergency Tank Next I", type: "Level Sensor" },
    "SL_LoRa_00007_01": { name: "New R&D Utility", type: "Flow Sensor" },
    "SL_LoRa_00007_02": { name: "PPE WashRoom", type: "Flow Sensor" },
    "SL_LoRa_00007_03": { name: "Production Scubber", type: "Flow Sensor" },
    "SL_LoRa_00007_04": { name: "R&D Furnance", type: "Level Sensor" },
    "SL_LoRa_00007_05": { name: "Next I ETP Tank", type: "Level Sensor" },
    "SL_LoRa_00007_06": { name: "Fire Hydrant Tank DG", type: "Level Sensor" },
    "SL_LoRa_00007_07": { name: "Main Utility Old Meter", type: "Flow Sensor" },
    "SL_LoRa_00008_01": { name: "Solar", type: "Flow Sensor" }
};


function getRelativeTime(timestamp) {
    const now = new Date();
    const diffMs = Math.abs(now - timestamp);
    const diffSeconds = Math.floor(diffMs / 1000);
    const diffMinutes = Math.floor(diffSeconds / 60);
    const diffHours = Math.floor(diffMinutes / 60);
    const diffDays = Math.floor(diffHours / 24);
    const diffMonths = Math.floor(diffDays / 30);
    const diffYears = Math.floor(diffDays / 365);

    if (diffSeconds < 60) return `${diffSeconds} second(s) ago`;
    if (diffMinutes < 60) return `${diffMinutes} minute(s) ago`;
    if (diffHours < 24) return `${diffHours} hour(s) ago`;
    if (diffDays < 30) return `${diffDays} day(s) ago`;
    if (diffMonths < 12) return `${diffMonths} month(s) ago`;
    return `${diffYears} year(s) ago`;
}



const fetchDeviceData = async () => {
    console.time('Execution Time');

    try {
        const latestQuery = `
            SELECT DISTINCT ON (deviceuid) deviceuid, timestamp
            FROM tms.actual_data
            WHERE deviceuid LIKE 'SL_LoRa%'
            ORDER BY deviceuid, timestamp DESC;
        `;

        const { rows: latestEntries } = await sourcePool.query(latestQuery);

        const formattedData = latestEntries.map(entry => {
            const utcDate = new Date(entry.timestamp.getTime());
            const istDate = new Date(utcDate.getTime() + 5.5 * 60 * 60 * 1000); // Add 5:30 offset
            const deviceInfo = deviceNameMap[entry.deviceuid] || { name: entry.deviceuid, type: 'Unknown' };

            return {
                name: deviceInfo.name,
                type: deviceInfo.type,
                deviceuid: entry.deviceuid,
                timestamp_utc: utcDate.toISOString(),
                timestamp_ist: istDate.toISOString().replace('T', ' ').slice(0, 19),
                relativeTime: getRelativeTime(utcDate),
                status: ((new Date() - utcDate) / (1000 * 60) > 30) ? 'Offline' : 'Online'
            };
        });

        // console.log(formattedData);
        sendAlert(formattedData);

    } catch (error) {
        console.error('Error fetching data:', error);
    }

    console.timeEnd('Execution Time');
};

async function sendAlert(data) {
    const templatePath = path.join(__dirname, "../elkem-mail/demo.ejs");

    fs.readFile(templatePath, "utf8", async (err, templateData) => {
        if (err) {
            console.error("Error reading email template:", err);
            return;
        }

        try {
            const compiledTemplate = ejs.compile(templateData);
            const emailHtml = compiledTemplate({ data });

            const mailOptions = {
                from: "donotreplysenselive@gmail.com",
                to: ['kpohekar19@gmail.com', 'abhijeet.bhoyar@senselive.io', 'shivani.francis@elkem.com', 'pradip.h.nikam@elkem.com', 'sarvan.rathi@elkem.com', 'murlidhar.w.lapkale@elkem.com'],
                subject: `Daily Report for Water Management`,
                html: emailHtml,
            };

            transporter.sendMail(mailOptions, (error, info) => {
                if (error) {
                    console.error(`Error sending email:`, error);
                } else {
                    console.log(`Alert sent to kpohekar19@gmail.com:`, info.response);
                }
            });
        } catch (templateError) {
            console.error("Template compilation error:", templateError);
        }
    });
}

cron.schedule('0 10 * * *', () => {
    fetchDeviceData();
}, {
    timezone: "Asia/Kolkata"
});

// cron.schedule('*/10 * * * *', () => {
//     fetchDeviceData();
// }, {
//     timezone: "Asia/Kolkata"
// });
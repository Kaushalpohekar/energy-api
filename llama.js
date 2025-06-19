const axios = require('axios');
const fs = require('fs');

const SENSOR_API = 'https://senso.senselive.io/api/data/SL02202355/intervals?interval=30day';
const TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VybmFtZSI6InNlbnNlbGl2ZUBlbGtlbS5jb20iLCJpYXQiOjE3NDYxNzMwNDMsImV4cCI6MTc0NjE3NjY0M30.gvIzjb--yMBPMQZrebS_rSA-cVcjFDlbH5emIT8L5-I';

async function fetchSensorData() {
  try {
    const res = await axios.get(SENSOR_API, {
      headers: {
        Authorization: `Bearer ${TOKEN}`
      }
    });
    return res.data;
  } catch (err) {
    console.error('❌ Failed to fetch sensor data:', err.message);
    return null;
  }
}

async function analyzeWithLLM(sensorDataSummary) {
  try {
    const res = await axios.post('http://localhost:11434/api/generate', {
      model: 'mistral',
      prompt: `Analysis the Furnace data :\n\n${sensorDataSummary}\n\nIdentify anomalies and suggest operational or maintenance recommendations.`,
      stream: false
    });
    return res.data.response;
  } catch (err) {
    console.error('❌ Failed to generate LLM analysis:', err.message);
    return 'LLM analysis failed.';
  }
}

function summarizeData(data) {
  if (!Array.isArray(data) || data.length === 0) return 'No data available.';

  const keys = Object.keys(data[0].values || {});
  const summaries = keys.map(key => {
    const values = data.map(entry => parseFloat(entry.values[key])).filter(v => !isNaN(v));
    const min = Math.min(...values);
    const max = Math.max(...values);
    const avg = values.reduce((a, b) => a + b, 0) / values.length;
    return `${key}: min=${min.toFixed(2)}, max=${max.toFixed(2)}, avg=${avg.toFixed(2)}`;
  });

  return summaries.join('\n');
}

async function main() {
  const rawData = await fetchSensorData();
  if (!rawData || !rawData.data) {
    console.error('❌ No valid sensor data received.');
    return;
  }

  const summary = summarizeData(rawData.data);
  const llmInsight = await analyzeWithLLM(summary);

  const output = `=== Sensor Data Summary ===\n${summary}\n\n=== LLM Analysis ===\n${llmInsight}`;

  fs.writeFileSync('sensor_analysis.txt', output);
  console.log('✅ Analysis saved to sensor_analysis.txt');
}

main();

require('dotenv').config();

const express = require('express');
const cron = require('node-cron');
const { runPipeline } = require('./pipeline');

const app = express();
const PORT = process.env.PORT || 5001;
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '0 9 * * *';

// Health endpoint — UptimeRobot pings this to keep Render awake
app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Manual trigger endpoint — useful for testing without waiting for the cron
app.get('/run', async (_req, res) => {
  console.log('[/run] Manual pipeline trigger');
  try {
    const result = await runPipeline();
    res.status(200).json({ success: true, ...result });
  } catch (err) {
    console.error('[/run] Pipeline error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Schedule the cron job
cron.schedule(CRON_SCHEDULE, async () => {
  console.log(`[cron] Firing on schedule: ${CRON_SCHEDULE}`);
  try {
    await runPipeline();
  } catch (err) {
    console.error('[cron] Pipeline error:', err.message);
  }
});

app.listen(PORT, () => {
  console.log(`[server] Listening on port ${PORT}`);
  console.log(`[server] Cron schedule: ${CRON_SCHEDULE}`);
});

const { queryLeadsAboveThreshold } = require('./bigquery');
const { getSentAccountIds, insertSentLeads } = require('./supabase');

const CREDIT_THRESHOLD = process.env.CREDIT_THRESHOLD || 60;
const CLAY_WEBHOOK_URL = process.env.CLAY_WEBHOOK_URL;
const RETRY_DELAY_MS = 5 * 60 * 1000; // 5 minutes

async function withRetry(fn, stepName) {
  try {
    return await fn();
  } catch (err) {
    console.warn(`[pipeline] ${stepName} failed: ${err.message}. Retrying in 5 minutes...`);
    await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
    try {
      return await fn();
    } catch (retryErr) {
      console.error(`[pipeline] FAILED: ${stepName} failed on retry: ${retryErr.message}. Ending run.`);
      throw retryErr;
    }
  }
}

async function runPipeline() {
  console.log(`[pipeline] Starting run at ${new Date().toISOString()}`);

  if (!CLAY_WEBHOOK_URL) {
    throw new Error('CLAY_WEBHOOK_URL is not set');
  }

  // Step 1: Query BigQuery
  console.log(`[pipeline] Querying BigQuery (threshold: ${CREDIT_THRESHOLD} credits)...`);
  const bqResults = await withRetry(
    () => queryLeadsAboveThreshold(CREDIT_THRESHOLD),
    'BigQuery query'
  );
  console.log(`[pipeline] BigQuery returned ${bqResults.length} leads above threshold`);

  if (bqResults.length === 0) {
    console.log('[pipeline] No leads above threshold — done');
    return { sent: 0, skipped: 0, total: 0 };
  }

  // Step 2: Get already-sent account_ids from Supabase
  console.log('[pipeline] Fetching already-sent account IDs from Supabase...');
  const sentAccountIds = await withRetry(
    () => getSentAccountIds(),
    'Supabase read'
  );
  console.log(`[pipeline] ${sentAccountIds.size} account(s) already sent`);

  // Step 3: Filter out already-sent
  const newLeads = bqResults.filter((lead) => !sentAccountIds.has(lead.account_id));
  console.log(`[pipeline] ${newLeads.length} net-new lead(s) to send (${bqResults.length - newLeads.length} skipped as already sent)`);

  if (newLeads.length === 0) {
    console.log('[pipeline] No new leads to send — done');
    return { sent: 0, skipped: bqResults.length, total: bqResults.length };
  }

  // Step 4: POST full batch to Clay webhook
  console.log(`[pipeline] POSTing ${newLeads.length} lead(s) to Clay webhook...`);
  await withRetry(async () => {
    const res = await fetch(CLAY_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(newLeads),
    });
    if (!res.ok) {
      const body = await res.text().catch(() => '(no body)');
      throw new Error(`Clay HTTP ${res.status}: ${body}`);
    }
  }, 'Clay webhook');

  // Step 5: On Clay success, write to Supabase
  console.log('[pipeline] Clay accepted the batch — writing to Supabase...');
  await withRetry(
    () => insertSentLeads(newLeads),
    'Supabase write'
  );
  console.log(`[pipeline] Done. Sent ${newLeads.length} lead(s) to Clay and logged to Supabase`);

  return {
    sent: newLeads.length,
    skipped: bqResults.length - newLeads.length,
    total: bqResults.length,
  };
}

module.exports = { runPipeline };

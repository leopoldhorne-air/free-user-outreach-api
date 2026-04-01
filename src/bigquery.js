const { BigQuery } = require('@google-cloud/bigquery');

// On Render, credentials come in as a JSON string env var rather than a file path.
// If GOOGLE_APPLICATION_CREDENTIALS_JSON is set, write it to a temp file so the
// BigQuery client can pick it up the standard way.
function resolveCredentials() {
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
    const fs = require('fs');
    const os = require('os');
    const path = require('path');
    const tmpPath = path.join(os.tmpdir(), 'bq-service-account.json');
    fs.writeFileSync(tmpPath, process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
    process.env.GOOGLE_APPLICATION_CREDENTIALS = tmpPath;
  }
}

resolveCredentials();

const bigquery = new BigQuery({
  projectId: process.env.BQ_PROJECT_ID,
  scopes: ['https://www.googleapis.com/auth/drive.readonly'],
});

const QUERY = `
WITH workspace_credits AS (
  SELECT
    workspace_id,
    SUM(micro_credits) / 1000000.0 AS workspace_total_credits
  FROM \`staging.stg_airdb__workspace_credit_consumptions\`
  WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY workspace_id
  HAVING SUM(micro_credits) / 1000000.0 > @threshold
),
capability_credits AS (
  SELECT
    c.workspace_id,
    c.account_id,
    c.capability,
    SUM(c.micro_credits) / 1000000.0 AS credits,
    COUNT(*) AS actions
  FROM \`staging.stg_airdb__workspace_credit_consumptions\` c
  INNER JOIN workspace_credits wc ON c.workspace_id = wc.workspace_id
  WHERE c.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY c.workspace_id, c.account_id, c.capability
),
user_totals AS (
  SELECT
    workspace_id,
    account_id,
    SUM(credits) AS total_credits,
    SUM(actions) AS total_actions
  FROM capability_credits
  GROUP BY workspace_id, account_id
),
sfdc_leads_deduped AS (
  SELECT email, lead_id,
    ROW_NUMBER() OVER (PARTITION BY LOWER(email) ORDER BY created_at ASC) AS rn
  FROM \`staging.stg_salesforce__leads\`
  WHERE email IS NOT NULL AND email != ''
),
sfdc_contacts_deduped AS (
  SELECT email, contact_id,
    ROW_NUMBER() OVER (PARTITION BY LOWER(email) ORDER BY created_at ASC) AS rn
  FROM \`staging.stg_salesforce__contacts\`
  WHERE email IS NOT NULL AND email != ''
)
SELECT
  ut.workspace_id,
  w.workspace_name,
  ut.account_id,
  w.air_plan,
  w.workspace_created_at,
  w.billable_accounts,
  wc.workspace_total_credits,
  ut.total_credits AS user_credits,
  ut.total_actions AS user_actions,
  MAX(CASE WHEN cc.capability = 'edit' THEN cc.credits END) AS edit_credits,
  MAX(CASE WHEN cc.capability = 'edit' THEN cc.actions END) AS edit_actions,
  MAX(CASE WHEN cc.capability = 'imageToVideo' THEN cc.credits END) AS i2v_credits,
  MAX(CASE WHEN cc.capability = 'imageToVideo' THEN cc.actions END) AS i2v_actions,
  MAX(CASE WHEN cc.capability = 'outpaint' THEN cc.credits END) AS outpaint_credits,
  MAX(CASE WHEN cc.capability = 'outpaint' THEN cc.actions END) AS outpaint_actions,
  MAX(CASE WHEN cc.capability = 'generate' THEN cc.credits END) AS generate_credits,
  MAX(CASE WHEN cc.capability = 'generate' THEN cc.actions END) AS generate_actions,
  MAX(CASE WHEN cc.capability = 'removeObject' THEN cc.credits END) AS removeobj_credits,
  MAX(CASE WHEN cc.capability = 'removeObject' THEN cc.actions END) AS removeobj_actions,
  MAX(CASE WHEN cc.capability = 'removeBackground' THEN cc.credits END) AS removebg_credits,
  MAX(CASE WHEN cc.capability = 'removeBackground' THEN cc.actions END) AS removebg_actions,
  MAX(CASE WHEN cc.capability = 'upscale' THEN cc.credits END) AS upscale_credits,
  MAX(CASE WHEN cc.capability = 'upscale' THEN cc.actions END) AS upscale_actions,
  MAX(CASE WHEN cc.capability = 'textToVideo' THEN cc.credits END) AS t2v_credits,
  MAX(CASE WHEN cc.capability = 'textToVideo' THEN cc.actions END) AS t2v_actions,
  MAX(CASE WHEN cc.capability = 'vectorize' THEN cc.credits END) AS vectorize_credits,
  MAX(CASE WHEN cc.capability = 'vectorize' THEN cc.actions END) AS vectorize_actions,
  a.email,
  a.first_name,
  a.last_name,
  a.phone,
  a.domain,
  sl.lead_id AS sfdc_lead_id,
  sc.contact_id AS sfdc_contact_id
FROM user_totals ut
JOIN workspace_credits wc ON ut.workspace_id = wc.workspace_id
JOIN \`analytics.fct_workspaces\` w ON ut.workspace_id = w.workspace_id
JOIN capability_credits cc
  ON ut.workspace_id = cc.workspace_id
  AND ut.account_id = cc.account_id
JOIN \`staging.stg_airdb__accounts\` a ON ut.account_id = a.account_id
LEFT JOIN sfdc_leads_deduped sl
  ON LOWER(a.email) = LOWER(sl.email)
  AND sl.rn = 1
LEFT JOIN sfdc_contacts_deduped sc
  ON LOWER(a.email) = LOWER(sc.email)
  AND sc.rn = 1
WHERE w.is_internal_workspace = FALSE
  AND LOWER(w.air_plan) = 'basic'
  AND w.workspace_created_at >= '2026-03-23'
  AND a.is_internal_account = FALSE
  AND LOWER(a.email) NOT LIKE '%@air.inc%'
  AND LOWER(a.email) NOT LIKE '%@aircamera.com%'
  AND LOWER(a.email) NOT LIKE '%+test%'
  AND ut.total_credits > 0
GROUP BY
  ut.workspace_id, w.workspace_name, ut.account_id, w.air_plan, w.workspace_created_at,
  wc.workspace_total_credits, w.billable_accounts, ut.total_credits, ut.total_actions,
  a.email, a.first_name, a.last_name, a.phone, a.domain,
  sl.lead_id, sc.contact_id
ORDER BY wc.workspace_total_credits DESC, ut.total_credits DESC
`;

async function queryLeadsAboveThreshold(threshold) {
  const options = {
    query: QUERY,
    params: { threshold: parseFloat(threshold) },
    location: 'US',
  };

  const [rows] = await bigquery.query(options);

  // BigQuery returns numeric types as objects with a value property in some cases;
  // normalize to plain JS values so downstream code can serialize them cleanly.
  return rows.map(normalizeRow);
}

function normalizeRow(row) {
  const out = {};
  for (const [key, val] of Object.entries(row)) {
    if (val === null || val === undefined) {
      out[key] = null;
    } else if (typeof val === 'object' && 'value' in val) {
      // BigQuery numeric/int64 wrapper
      out[key] = Number(val.value);
    } else if (val instanceof Date) {
      out[key] = val.toISOString();
    } else if (typeof val === 'object' && val.constructor?.name === 'BigQueryTimestamp') {
      out[key] = val.value;
    } else {
      out[key] = val;
    }
  }
  return out;
}

module.exports = { queryLeadsAboveThreshold };

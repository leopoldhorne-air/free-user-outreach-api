require('dotenv').config();

const { BigQuery } = require('@google-cloud/bigquery');

const bigquery = new BigQuery({
  projectId: process.env.BQ_PROJECT_ID,
  scopes: ['https://www.googleapis.com/auth/drive.readonly'],
});

async function main() {
  console.log('Testing BigQuery connection...');

  const queries = [
    { name: 'stg_airdb__workspace_credit_consumptions', sql: 'SELECT COUNT(*) as cnt FROM `staging.stg_airdb__workspace_credit_consumptions` LIMIT 1' },
    { name: 'stg_airdb__accounts', sql: 'SELECT COUNT(*) as cnt FROM `staging.stg_airdb__accounts` LIMIT 1' },
    { name: 'fct_workspaces', sql: 'SELECT COUNT(*) as cnt FROM `analytics.fct_workspaces` LIMIT 1' },
    { name: 'staging.stg_salesforce__leads', sql: 'SELECT COUNT(*) as cnt FROM `staging.stg_salesforce__leads` LIMIT 1' },
    { name: 'analytics.stg_salesforce__leads', sql: 'SELECT COUNT(*) as cnt FROM `analytics.stg_salesforce__leads` LIMIT 1' },
    { name: 'stg_salesforce__contacts', sql: 'SELECT COUNT(*) as cnt FROM `staging.stg_salesforce__contacts` LIMIT 1' },
  ];

  for (const q of queries) {
    try {
      const [rows] = await bigquery.query({ query: q.sql, location: 'US' });
      console.log(`✅ ${q.name}: OK (${rows[0].cnt} rows)`);
    } catch (err) {
      console.log(`❌ ${q.name}: ${err.message}`);
    }
  }
}

main().catch(console.error);

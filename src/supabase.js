const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_PUBLISHABLE_KEY
);

async function getSentAccountIds() {
  const { data, error } = await supabase
    .from('sent_leads')
    .select('account_id');

  if (error) throw new Error(`Supabase read failed: ${error.message}`);

  return new Set(data.map((r) => r.account_id));
}

async function insertSentLeads(leads) {
  const rows = leads.map((lead) => ({
    account_id: lead.account_id,
    workspace_id: lead.workspace_id ?? null,
    email: lead.email,
    first_name: lead.first_name ?? null,
    last_name: lead.last_name ?? null,
    domain: lead.domain ?? null,
    total_credits: lead.user_credits ?? null,
    sfdc_lead_id: lead.sfdc_lead_id ?? null,
    sfdc_contact_id: lead.sfdc_contact_id ?? null,
  }));

  const { error } = await supabase.from('sent_leads').insert(rows);

  if (error) throw new Error(`Supabase insert failed: ${error.message}`);
}

module.exports = { getSentAccountIds, insertSentLeads };

require('dotenv').config();

const CLAY_WEBHOOK_URL = process.env.CLAY_WEBHOOK_URL;

const testPayload = [
  {
    workspace_id: "8bd703dd-1532-4363-8a4b-2cc8de945281",
    workspace_name: "Wakefit",
    air_plan: "basic",
    workspace_created_at: "2026-03-23T08:33:12",
    workspace_creator_account_id: "8f19fb82-8e1e-47e6-9a17-19eead50299f",
    account_id: "8f19fb82-8e1e-47e6-9a17-19eead50299f",
    total_credits: 739.355455,
    total_actions: 40,
    edit_credits: 27.771434,
    edit_actions: 4,
    i2v_credits: 87.012575,
    i2v_actions: 5,
    outpaint_credits: 13.714296,
    outpaint_actions: 2,
    generate_credits: 5.71432,
    generate_actions: null,
    removeobj_credits: null,
    removeobj_actions: null,
    removebg_credits: null,
    removebg_actions: null,
    upscale_credits: 5.142865,
    upscale_actions: null,
    t2v_credits: null,
    t2v_actions: null,
    vectorize_credits: null,
    vectorize_actions: null,
    email: "sachin.bv@wakefit.co",
    first_name: "Sachin",
    last_name: "B",
    phone: null,
    domain: "wakefit.co",
    sfdc_account_id: "001Qq0000181fLmIAI",
    sfdc_account_name: "Wakefit",
    crm_status: "In CRM"
  }
];

async function main() {
  console.log(`POSTing test payload to: ${CLAY_WEBHOOK_URL}`);
  const res = await fetch(CLAY_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(testPayload),
  });
  const body = await res.text();
  console.log(`Status: ${res.status}`);
  console.log(`Response: ${body}`);
}

main().catch(console.error);

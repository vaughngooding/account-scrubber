import openai
from supabase import create_client, Client

# ---🔐 Supabase Config --- #
SUPABASE_URL = "https://wwhebjlhutrkgyoprdus.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3aGViamxodXRya2d5b3ByZHVzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA4OTQzNjgsImV4cCI6MjA2NjQ3MDM2OH0.IBel8gCdfa-64ggNt9hFp2VohS7Lqfi-mBEgoQbUaBo"
SOURCE_TABLE = "TAE Account List"           # ✅ Updated source
DEST_TABLE = "TAE Account Scrub FY26"     # ✅ Updated destination

# ---🔐 OpenAI Config --- #
OPENAI_API_KEY = "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"
client = openai.OpenAI(api_key=OPENAI_API_KEY)

# ---📦 Supabase Client Init --- #
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🔍 Connecting to Supabase and loading accounts...")
response = supabase.table(SOURCE_TABLE).select("*").execute()
accounts = response.data
# ⛔️ Remove already-scrubbed accounts (by SFDC ID)
scrubbed_resp = supabase.table(DEST_TABLE).select("account_id").execute()
scrubbed_ids = {r["account_id"] for r in (scrubbed_resp.data or []) if r.get("account_id")}

accounts = [
    acct for acct in accounts
    if acct.get("SFDC ID") and acct.get("SFDC ID") not in scrubbed_ids
]
print(f"🧼 {len(accounts)} accounts left to scrub after filtering out {len(scrubbed_ids)} previously scrubbed.")
if not accounts:
    print("No accounts found to scrub.")
    exit()

print(f"✅ Loaded {len(accounts)} accounts from Supabase")
print("🧪 First 3 sample accounts:", accounts[:3])

# ---✍️ GPT Function --- #
def analyze_company(account_name, website):
    prompt = f"""
You're a GPT-4 assistant tasked with researching companies \"{account_name}\" to determine whether they've been acquired, merged, renamed, shut down, has subsidiaries or are government, academic, or healthcare organizations. Return a short response if any of these things are true.

Rules:
- Academic, government, or healthcare → not sellable.
- Acquired/renamed/merged/shut down → may not be sellable; flag for follow-up.
- Subsidiaries → note so AEs can claim child accounts.

The output should be short. Do not restate the question or explain your process.

Example outputs:  
- Rebranded. Originally named CartoDB, the company rebranded to CARTO in 2012 to reflect a shift to location intelligence software.  
- Several subsidiaries including Westmont Hospitality Management, Gooding Hotels, and Fairchild Hotels.  
- No acquisitions, merges, or subsidiaries.  
- 2 subsidiaries: S3 Controls and Wurtec Canada.
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ GPT Error analyzing {account_name}: {e}")
        return "GPT ERROR"

# ---🔁 Loop Through Accounts --- #
for account in accounts:
    account_name = account.get("Account Name")
    website = account.get("Website")
    linkedin_company_id = account.get("LinkedIn Company ID")
    account_id = account.get("SFDC ID")
    ae_name = account.get("ae")

    print(f"🔎 Scrubbing: {account_name} | Website: {website} | LinkedIn ID: {linkedin_company_id} | AE: {ae_name}")

    summary = analyze_company(account_name, website)

    try:
        supabase.table(DEST_TABLE).insert({
            "account_name": account_name,
            "website": website,
            "linkedin_company_id": linkedin_company_id,
            "scrub_summary": summary,
            "account_id": account_id,
            "ae": ae_name
        }).execute()
        print(f"✅ Saved result for {account_name}")
    except Exception as e:
        print(f"❌ Error saving result for {account_name}: {e}")
import time
import openai
from supabase import create_client, Client

# ---🔐 Supabase Config --- #
SUPABASE_URL = "https://wwhebjlhutrkgyoprdus.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3aGViamxodXRya2d5b3ByZHVzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA4OTQzNjgsImV4cCI6MjA2NjQ3MDM2OH0.IBel8gCdfa-64ggNt9hFp2VohS7Lqfi-mBEgoQbUaBo"
SOURCE_TABLE = "TAE Account List"
DEST_TABLE = "TAE Account Scrub FY26"

# ---🔐 Perplexity Config --- #
PERPLEXITY_API_KEY = "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"

# ---⚙️ Batch Config --- #
BATCH_SIZE = 3  # Number of companies to analyze per API call (conservative for web search)
MAX_RETRIES = 3  # Maximum retries for failed API calls

# Note: Perplexity has 50 requests/min rate limit and needs tokens for web search results
# Start with 3 companies per batch, increase to 5 if performance is good

# Initialize Perplexity/OpenAI Client
client = openai.OpenAI(
    api_key=PERPLEXITY_API_KEY,
    base_url="https://api.perplexity.ai"
)

# ---📦 Supabase Client Init --- #
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🔍 Connecting to Supabase and loading accounts...")
response = supabase.table(SOURCE_TABLE).select("*").execute()
accounts = response.data

# Filter already-scrubbed accounts
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
print(f"📦 Will process in batches of {BATCH_SIZE}")

# ---🧠 Batch Analysis Function --- #
def analyze_companies_batch(batch_accounts):
    """Analyze multiple companies in a single API call"""
    
    # Build the batch prompt
    company_list = []
    for i, account in enumerate(batch_accounts, 1):
        account_name = account.get("Account Name", "Unknown")
        website = account.get("Website", "No website")
        company_list.append(f"{i}. {account_name} ({website})")
    
    companies_text = "\n".join(company_list)
    
    prompt = f"""
Analyze these {len(batch_accounts)} companies and summarize only if any of these are true for each:
    • Acquired, merged, renamed, shut down
    • Has subsidiaries  
    • Is a government, academic, or healthcare organization

Companies to analyze:
{companies_text}

For each company, output ONLY a one-line status using these templates:
    • Company is a subsidiary of Parent
    • Acquired by Buyer in Year
    • Renamed to New Name
    • Shut down
    • Academic/government/healthcare: not sellable
    • No acquisitions, merges, renames, shutdowns, or subsidiaries

Format your response as:
1. [Status for company 1]
2. [Status for company 2]
3. [Status for company 3]
...

Write nothing else. No explanations, just numbered statuses.
"""
    
    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model="sonar",  # or "sonar-pro" for higher quality
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that analyzes company statuses."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=2000  # Increase for batch responses
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"❌ Attempt {attempt + 1} failed: {e}")
            if attempt == MAX_RETRIES - 1:
                print(f"❌ All {MAX_RETRIES} attempts failed for batch")
                return None
            time.sleep(2 ** attempt)  # Exponential backoff

def parse_batch_results(batch_response, batch_accounts):
    """Parse the batch response and match results to accounts"""
    if not batch_response:
        return ["PERPLEXITY ERROR"] * len(batch_accounts)
    
    results = []
    lines = batch_response.split('\n')
    
    # Extract numbered results
    for i in range(len(batch_accounts)):
        found_result = False
        for line in lines:
            line = line.strip()
            if line.startswith(f"{i+1}."):
                # Remove the number prefix and clean up
                result = line[len(f"{i+1}."):].strip()
                results.append(result)
                found_result = True
                break
        
        if not found_result:
            print(f"⚠️ Could not find result for company {i+1}, using error placeholder")
            results.append("PARSING ERROR - MANUAL REVIEW NEEDED")
    
    return results

# ---📊 Batch Processing Loop --- #
def process_batches(accounts):
    total_batches = (len(accounts) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(0, len(accounts), BATCH_SIZE):
        current_batch = batch_num // BATCH_SIZE + 1
        batch_accounts = accounts[batch_num:batch_num + BATCH_SIZE]
        
        print(f"\n📦 Processing batch {current_batch}/{total_batches} ({len(batch_accounts)} companies)")
        
        # Show what we're processing
        for account in batch_accounts:
            print(f"   • {account.get('Account Name')} - {account.get('Website')}")
        
        # Analyze the batch
        print(f"🔎 Analyzing batch {current_batch}...")
        batch_response = analyze_companies_batch(batch_accounts)
        
        if batch_response:
            print(f"✅ Received batch analysis for {len(batch_accounts)} companies")
            # Parse results
            results = parse_batch_results(batch_response, batch_accounts)
            
            # Save results to Supabase
            for account, summary in zip(batch_accounts, results):
                try:
                    supabase.table(DEST_TABLE).insert({
                        "account_name": account.get("Account Name"),
                        "website": account.get("Website"),
                        "linkedin_company_id": account.get("LinkedIn Company ID"),
                        "scrub_summary": summary,
                        "account_id": account.get("SFDC ID"),
                        "ae": account.get("ae")
                    }).execute()
                    print(f"   ✅ Saved: {account.get('Account Name')} -> {summary}")
                except Exception as e:
                    print(f"   ❌ Error saving {account.get('Account Name')}: {e}")
        else:
            print(f"❌ Failed to analyze batch {current_batch}, skipping...")
            continue
        
        # Rate limiting pause between batches (Perplexity: 50 requests/min = 1.2s minimum)
        if current_batch < total_batches:
            print(f"⏱️ Pausing 1.4 seconds before next batch...")
            time.sleep(1.4)  # Just above the 1.2s minimum for 50 req/min

# ---🚀 Execute Batch Processing --- #
print(f"🚀 Starting batch processing of {len(accounts)} accounts...")
process_batches(accounts)
print("\n🎉 Batch processing complete!")
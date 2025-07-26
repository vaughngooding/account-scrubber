import openai
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- CONFIG ---
SUPABASE_URL = "https://wwhebjlhutrkgyoprdus.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3aGViamxodXRya2d5b3ByZHVzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA4OTQzNjgsImV4cCI6MjA2NjQ3MDM2OH0.IBel8gCdfa-64ggNt9hFp2VohS7Lqfi-mBEgoQbUaBo"
OPENAI_API_KEY = "your-openai-key-here"  # <--- Replace this with your actual API key
CSV_FILE = "Vaughn's LinkedIn Accounts FY26.csv"
TABLE_NAME = "company_status_results"
COLUMN_COMPANY_NAME = "account_name"
COLUMN_SUMMARY = "scrub_summary"

# --- INIT ---
openai.api_key = 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
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- FUNCTIONS ---
def search_google(company):
    query = f"{company} company news acquisition OR merger OR rename OR shutdown"
    url = f"https://www.google.com/search?q={requests.utils.quote(query)}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, "html.parser")
    results = soup.select(".tF2Cxc")
    snippets = []
    for r in results[:3]:
        title = r.select_one("h3").text if r.select_one("h3") else ""
        link = r.select_one("a")["href"]
        snippet = r.select_one(".VwiC3b").text if r.select_one(".VwiC3b") else ""
        snippets.append(f"{title}\n{snippet}\n{link}")
    return "\n\n".join(snippets)

def ask_chatgpt(company, context):
    prompt = f"""You are a research analyst. Based on the snippets below, is there evidence this company has been acquired, renamed, merged, or no longer exists? If unclear, say so.
Company: {company}
Snippets:\n{context}
Return a 2-3 sentence summary with links if relevant."""
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message["content"].strip()

# --- RUN ---
df = pd.read_csv(CSV_FILE)
for _, row in df.iterrows():
    company = row["Account Name"]
    print(f"🔍 Checking: {company}")
    try:
        search_results = search_google(company)
        time.sleep(2)  # avoid rate limits
        summary = ask_chatgpt(company, search_results)
    except Exception as e:
        summary = f"Error: {e}"
    
    supabase.table(TABLE_NAME).insert({
        COLUMN_COMPANY_NAME: company,
        COLUMN_SUMMARY: summary
    }).execute()

print("✅ All done. Results uploaded to Supabase.")
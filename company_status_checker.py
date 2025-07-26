import os
import openai
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # If using OpenAI
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")

CSV_FILE = "Vaughn's LinkedIn Accounts FY26.csv"
TABLE_NAME = "company_status_results"
COLUMN_COMPANY_NAME = "account_name"
COLUMN_SUMMARY = "scrub_summary"

# Initialize clients
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
client = openai.OpenAI(api_key=OPENAI_API_KEY)

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
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message.content.strip()

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
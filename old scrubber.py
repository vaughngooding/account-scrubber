from __future__ import annotations

"""
Supabase ↔ Perplexity Company Research Automation

… (omitted for brevity) …
"""

# [imports remain unchanged]

# ------------------------------------------------------------------
# --- ✨ UPDATED CONFIG: NEW TABLES & API KEYS -----------------------
# ------------------------------------------------------------------
DEFAULT_SUPABASE_URL = "https://wwhebjlhutrkgyoprdus.supabase.co"
DEFAULT_SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3aGViamxodXRya2d5b3ByZHVzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA4OTQzNjgsImV4cCI6MjA2NjQ3MDM2OH0.IBel8gCdfa-64ggNt9hFp2VohS7Lqfi-mBEgoQbUaBo"
SOURCE_TABLE = "TAE Account List"
DEST_TABLE = "TAE Account Scrub FY26"

DEFAULT_PPLX_API_KEY = "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"
PPLX_API_URL = "https://api.perplexity.ai/chat/completions"
DEFAULT_PPLX_MODEL = "sonar-pro"

# ------------------------------------------------------------------
# (rest of the script remains unchanged, using updated SOURCE_TABLE and DEST_TABLE)
# ------------------------------------------------------------------

if __name__ == "__main__":
    main()


"""
Supabase ↔ Perplexity Company Research Automation
=================================================

Batch (or single-record) automation that:
 1. Pulls account records from a SOURCE_TABLE in Supabase.
 2. For each account, calls the Perplexity Chat Completions API to perform live web research
    (Google/LinkedIn/news/company site via Perplexity's online models).
 3. Applies a strict, short, sales-facing response format based on your rules.
 4. Writes results into a DEST_TABLE in Supabase (upsert), including:
      - account_id (from SOURCE_TABLE column `SFDC ID`)
      - account_name
      - website
      - ae  (for AE attribution/context)
      - scrub_summary (LLM response text)
      - researched_on (UTC timestamp when row written)

You can run this as a CLI script:

    python supabase_perplexity_company_research_automation.py \
        --mode batch \
        --limit 50 \
        --skip-if-has-summary

Or to process a single account id:

    python supabase_perplexity_company_research_automation.py --account-id 001xx000018AbCd

Environment Variables (recommended; fall back to hard-coded defaults below if unset):
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY   (preferred) or SUPABASE_KEY (anon ok if RLS permits inserts to DEST_TABLE)
    PPLX_API_KEY                (Perplexity)

Schema Assumptions -------------------------------------------------------
SOURCE_TABLE = "Vaughn LinkedIn Accounts"
  Required columns (case-insensitive match handled in code):
    "SFDC ID"         -> unique external account id (string) **used as account_id**
    "account_name"    -> company name
    "website"         -> canonical company website (nullable)
    "ae"              -> AE name/owner (nullable)

DEST_TABLE = "FY26 Account Scrub"
  Columns (will be created if not present *if you enable --auto-migrate*; else must exist):
    account_id TEXT PRIMARY KEY
    account_name TEXT
    website TEXT
    ae TEXT
    scrub_summary TEXT
    num_jobs_est INT NULL
    num_jobs_over_80k_est INT NULL
    researched_on TIMESTAMPTZ DEFAULT now()

NOTE: DDL auto-migration requires a service key; by default script does NOT attempt schema changes.

--------------------------------------------------------------------------
Perplexity API Notes
--------------------------------------------------------------------------
Endpoint: https://api.perplexity.ai/chat/completions
Docs: https://docs.perplexity.ai (models, usage)

We use streamed=False JSON POST. Example payload:
{
  "model": "sonar-pro",            # or another online model able to browse
  "messages": [
      {"role": "system", "content": SYSTEM_PROMPT},
      {"role": "user",   "content": user_prompt}
  ],
  "temperature": 0.1,
  "max_tokens": 256,
  "return_citations": true
}

We then parse `choices[0].message.content` for the short summary.

--------------------------------------------------------------------------
Response Formatting Guardrails
--------------------------------------------------------------------------
We strongly steer the model with formatting rules + length targets:
  • Output MUST be ≤ 300 characters (soft); we hard-truncate at 500 chars to be safe.
  • Single concise sentence preferred; two max.
  • Use leading keyword cues (e.g., "Acquired", "Rebranded", "Subsidiaries", "No red flags").
  • No explanation of process; no bullet formatting characters required (we'll store plain text).

--------------------------------------------------------------------------
Parsing Job Counts (Optional heuristic)
--------------------------------------------------------------------------
If the model includes a pattern like: "Jobs: 42 (≥$80k: 10)" or "LinkedIn ~42 open roles; ~10 over $80k", we regex-extract. If absent, leave null.

--------------------------------------------------------------------------
Rate Limiting
--------------------------------------------------------------------------
Default: sleep 1.1s between Perplexity calls; configurable.

--------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import typing as t
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from supabase import create_client, Client

# ------------------------------------------------------------------
# --- User Config Defaults (overridable by env vars / CLI) ---------
# ------------------------------------------------------------------
DEFAULT_SUPABASE_URL = "https://wwhebjlhutrkgyoprdus.supabase.co"
DEFAULT_SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3aGViamxodXRya2d5b3ByZHVzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA4OTQzNjgsImV4cCI6MjA2NjQ3MDM2OH0.IBel8gCdfa-64ggNt9hFp2VohS7Lqfi-mBEgoQbUaBo"
SOURCE_TABLE = "Vaughn LinkedIn Accounts"
DEST_TABLE = "FY26 Account Scrub"

DEFAULT_PPLX_API_KEY = "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"
PPLX_API_URL = "https://api.perplexity.ai/chat/completions"
DEFAULT_PPLX_MODEL = "sonar-pro"  # change if needed

# ------------------------------------------------------------------
# Prompt Templates --------------------------------------------------
# ------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a focused web research assistant for a SaaS sales team. "
    "Given a company name and website, quickly check the public web (Google, LinkedIn, news, the site) and return a SHORT factual note ONLY if any of the following apply: \n"
    "• Academic / government / healthcare (mark 'Not sellable').\n"
    "• Acquired / merged / renamed / shut down (flag).\n"
    "• Subsidiaries exist (list a few; flag for claiming child accounts).\n\n"
    "If none apply, say 'No acquisitions, merges, or subsidiaries.'\n\n"
    "STYLE RULES:\n"
    "- <=300 chars; 1 short sentence; 2 max.\n"
    "- No preamble, no explanation, no markdown bullets.\n"
    "- Capitalize first word (e.g., 'Acquired', 'Rebranded', 'Several subsidiaries', 'Not sellable').\n"
    "- Include year if known (e.g., 'Acquired by XYZ in 2023.').\n"
    "- Optional job counts: 'Jobs: 42 (>80k: 10)'.\n"
)

USER_PROMPT_TEMPLATE = (
    "Company: {account_name}\n"
    "Website: {website}\n"
    "Please respond per the style rules."
)

# ------------------------------------------------------------------
# Data Classes ------------------------------------------------------
# ------------------------------------------------------------------
@dataclass
class Account:
    account_id: str
    account_name: str
    website: t.Optional[str] = None
    ae: t.Optional[str] = None


@dataclass
class ScrubResult:
    account_id: str
    account_name: str
    website: t.Optional[str]
    ae: t.Optional[str]
    scrub_summary: str
    num_jobs_est: t.Optional[int] = None
    num_jobs_over_80k_est: t.Optional[int] = None
    researched_on: datetime = datetime.now(timezone.utc)


# ------------------------------------------------------------------
# Utility: env getters ----------------------------------------------
# ------------------------------------------------------------------

def env_or_default(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val not in (None, "") else default


# ------------------------------------------------------------------
# Supabase Helpers --------------------------------------------------
# ------------------------------------------------------------------

def get_supabase_client(url: str, key: str) -> Client:
    return create_client(url, key)


def fetch_accounts(
    supa: Client,
    limit: t.Optional[int] = None,
    offset: int = 0,
    only_missing: bool = False,
) -> t.List[Account]:
    """Fetch accounts from SOURCE_TABLE.

    Parameters
    ----------
    only_missing: if True, skip rows that already have a scrub record in DEST_TABLE.
    """
    # Pull source rows
    query = supa.table(SOURCE_TABLE).select("*")
    if limit:
        query = query.limit(limit)
    if offset:
        query = query.range(offset, offset + (limit or 100000) - 1)
    resp = query.execute()
    rows = resp.data or []

    # Build a lookup of already-scrubbed ids if filtering
    existing_ids = set()
    if only_missing:
        existing_resp = supa.table(DEST_TABLE).select("account_id").execute()
        existing_ids = {r["account_id"] for r in (existing_resp.data or []) if r.get("account_id")}

    accounts: t.List[Account] = []
    for r in rows:
        acct_id = str(r.get("SFDC ID") or r.get("sfdc_id") or r.get("account_id") or "").strip()
        if not acct_id:
            continue  # skip rows that can't be keyed
        if only_missing and acct_id in existing_ids:
            continue
        name = (r.get("account_name") or r.get("Account Name") or r.get("name") or "").strip()
        website = (r.get("website") or r.get("Website") or None)
        ae = (r.get("ae") or r.get("AE") or None)
        accounts.append(Account(account_id=acct_id, account_name=name, website=website, ae=ae))
    return accounts


def upsert_scrub_result(supa: Client, result: ScrubResult) -> None:
    payload = {
        "account_id": result.account_id,
        "account_name": result.account_name,
        "website": result.website,
        "ae": result.ae,
        "scrub_summary": result.scrub_summary,
        "#_of_jobs": result.num_jobs_est,
        "#_of_jobs_over_$80k": result.num_jobs_over_80k_est,
        "researched_on": result.researched_on.isoformat(),
    }
    # NB: Column names in Supabase can't include "$" easily; if you already created them, you may need to quote.
    # For robustness we map to snake_case alt names too, if present.
    alt_payload = {
        "num_jobs_est": result.num_jobs_est,
        "num_jobs_over_80k_est": result.num_jobs_over_80k_est,
    }
    payload.update(alt_payload)

    supa.table(DEST_TABLE).upsert(payload, on_conflict="account_id").execute()


# ------------------------------------------------------------------
# Perplexity Call ---------------------------------------------------
# ------------------------------------------------------------------

def build_user_prompt(account_name: str, website: t.Optional[str]) -> str:
    return USER_PROMPT_TEMPLATE.format(account_name=account_name, website=website or "N/A")


def call_perplexity(
    api_key: str,
    account_name: str,
    website: t.Optional[str],
    model: str = DEFAULT_PPLX_MODEL,
    temperature: float = 0.1,
    max_tokens: int = 256,
    timeout: int = 60,
    return_citations: bool = True,
) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(account_name, website)},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "return_citations": return_citations,
    }
    resp = requests.post(PPLX_API_URL, headers=headers, json=payload, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"Perplexity API error {resp.status_code}: {resp.text[:400]}")
    data = resp.json()
    try:
        content = data["choices"][0]["message"]["content"].strip()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Unexpected Perplexity response shape: {e}; payload={data}")
    # Hard truncate safeguard
    if len(content) > 500:
        content = content[:497].rstrip() + "..."
    return content


# ------------------------------------------------------------------
# Parsing Helpers ---------------------------------------------------
# ------------------------------------------------------------------
JOBS_RE = re.compile(r"Jobs?:\s*(~?\d+)(?:[^\d>]*[>≥]?\$?80k[:\s]*~?(\d+))?", re.IGNORECASE)
ALT_JOBS_RE = re.compile(r"(~?\d+)\s+open\s+roles", re.IGNORECASE)
ALT_HIGH_PAY_RE = re.compile(r"(~?\d+)\s*(?:over|>)[\s$]*80k", re.IGNORECASE)


def parse_job_counts(text: str) -> t.Tuple[t.Optional[int], t.Optional[int]]:
    m = JOBS_RE.search(text)
    if m:
        total = int(m.group(1).lstrip("~"))
        high = m.group(2)
        return total, (int(high.lstrip("~")) if high else None)
    # fallback patterns
    total = None
    high = None
    m = ALT_JOBS_RE.search(text)
    if m:
        total = int(m.group(1).lstrip("~"))
    m2 = ALT_HIGH_PAY_RE.search(text)
    if m2:
        high = int(m2.group(1).lstrip("~"))
    return total, high


# ------------------------------------------------------------------
# Core Scrub --------------------------------------------------------
# ------------------------------------------------------------------

def scrub_account(api_key: str, acct: Account, retry: int = 2, backoff: float = 2.0) -> ScrubResult:
    last_err: t.Optional[Exception] = None
    for attempt in range(retry + 1):
        try:
            resp_text = call_perplexity(api_key, acct.account_name, acct.website)
            total, high = parse_job_counts(resp_text)
            return ScrubResult(
                account_id=acct.account_id,
                account_name=acct.account_name,
                website=acct.website,
                ae=acct.ae,
                scrub_summary=resp_text,
                num_jobs_est=total,
                num_jobs_over_80k_est=high,
            )
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < retry:
                time.sleep(backoff * (2 ** attempt))
            else:
                # final failure: capture error text as scrub_summary
                return ScrubResult(
                    account_id=acct.account_id,
                    account_name=acct.account_name,
                    website=acct.website,
                    ae=acct.ae,
                    scrub_summary=f"ERROR: {e}",
                )
    # unreachable, but type checkers
    raise RuntimeError(str(last_err))


# ------------------------------------------------------------------
# Batch Runner ------------------------------------------------------
# ------------------------------------------------------------------

def run_batch(
    supa: Client,
    api_key: str,
    limit: t.Optional[int],
    offset: int,
    only_missing: bool,
    sleep_s: float,
) -> None:
    accounts = fetch_accounts(supa, limit=limit, offset=offset, only_missing=only_missing)
    print(f"Fetched {len(accounts)} accounts to process.")
    for i, acct in enumerate(accounts, 1):
        print(f"[{i}/{len(accounts)}] {acct.account_name} ({acct.account_id}) ...", end=" ")
        res = scrub_account(api_key, acct)
        upsert_scrub_result(supa, res)
        print("done.")
        time.sleep(sleep_s)


# ------------------------------------------------------------------
# Single Runner -----------------------------------------------------
# ------------------------------------------------------------------

def run_single(supa: Client, api_key: str, account_id: str) -> None:
    # fetch the one account by SFDC ID
    resp = (
        supa.table(SOURCE_TABLE)
        .select("*")
        .eq("SFDC ID", account_id)
        .single()
        .execute()
    )
    if not resp.data:
        print(f"No account found for SFDC ID {account_id}")
        return
    r = resp.data
    acct = Account(
        account_id=str(r.get("SFDC ID")),
        account_name=(r.get("account_name") or r.get("Account Name") or "").strip(),
        website=(r.get("website") or r.get("Website") or None),
        ae=(r.get("ae") or r.get("AE") or None),
    )
    res = scrub_account(api_key, acct)
    upsert_scrub_result(supa, res)
    print(res.scrub_summary)


# ------------------------------------------------------------------
# CLI ---------------------------------------------------------------
# ------------------------------------------------------------------

def parse_args(argv: t.Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Supabase ↔ Perplexity Company Research Automation")
    m = p.add_mutually_exclusive_group(required=False)
    m.add_argument("--mode", choices=["batch", "single"], default="batch")
    m.add_argument("--account-id", help="Process a single SFDC ID (implies --mode single)")

    p.add_argument("--limit", type=int, default=None, help="Max # source rows to process")
    p.add_argument("--offset", type=int, default=0, help="Row offset into source table")
    p.add_argument("--only-missing", action="store_true", help="Skip accounts already scrubbed")
    p.add_argument("--sleep", type=float, default=1.1, help="Seconds to sleep between Perplexity calls")

    p.add_argument("--supabase-url", default=env_or_default("SUPABASE_URL", DEFAULT_SUPABASE_URL))
    p.add_argument("--supabase-key", default=env_or_default("SUPABASE_SERVICE_ROLE_KEY", env_or_default("SUPABASE_KEY", DEFAULT_SUPABASE_KEY)))
    p.add_argument("--pplx-key", default=env_or_default("PPLX_API_KEY", DEFAULT_PPLX_API_KEY))
    p.add_argument("--pplx-model", default=env_or_default("PPLX_MODEL", DEFAULT_PPLX_MODEL))

    return p.parse_args(argv)


def main(argv: t.Sequence[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])

    # Determine mode
    mode = "single" if args.account_id else args.mode

    # Init Supabase
    supa = get_supabase_client(args.supabase_url, args.supabase_key)

    if mode == "single":
        run_single(supa, args.pplx_key, args.account_id)
    else:
        run_batch(
            supa,
            args.pplx_key,
            limit=args.limit,
            offset=args.offset,
            only_missing=args.only_missing,
            sleep_s=args.sleep,
        )


if __name__ == "__main__":
    main()

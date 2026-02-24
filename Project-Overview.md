# Project Overview: Medium Stealth Bot

## 1. Mission & Philosophy

The **Medium Stealth Bot** is a lightweight, "local-first" CLI tool meticulously engineered to automate personal Medium network management. 

Instead of treating automation purely as data processing, this system places **stealth and human emulation** at its absolute core.

* **Stealth-First:** The bot explicitly avoids acting like a headless script or naive machine. It meticulously mimics the specific TLS fingerprints, headers, GraphQL mutation patterns, and procedural flows of a legitimate human user interacting via a Chrome (`chrome120`) browser.
* **Pragmatic Automation:** It eschews complex architectures, external servers, or distributed orchestration. It operates as a strict, self-contained, daily CLI script ("The Daily Habit") that systematically spends a pre-defined "budget" of organic-looking actions before immediately exiting.
* **Human Empathy & Platform Respect:** It honors the target platform metrics by introducing unpredictable "Wait" times, legitimate "Reading" pauses, sequential request resolutions, and organic discovery paths (such as Referer chains and tag queries).

---

## 2. The "Stealth" Architecture

The system utilizes a **Hybrid Handover** architecture. This separates the difficult, human-centric authentication problem from the lightweight, scalable execution engine.

### The Stack

* **Language:** Python 3.11+
* **Authentication:** `Camoufox` (Stealth Firefox branch) — *Relied upon rigidly for **only** the initial login phase to bypass strict interactive Cloudflare/recaptcha challenges.*
* **Network Layer:** `curl-cffi` — *Takes over post-login, impersonating the exact Chrome TLS and JA3 fingerprints to mask programmatic origin for all GraphQL/API requests.*
* **State & Storage:** `SQLite` (Standard `sqlite3`) — *A self-contained, single-file database for tracking user targets, relationships, and action budgets.*
* **CLI Interface:** `Typer` + `Rich` — *Providing a distinct, modern terminal UI for easy manual or cron-based invocation.*

### The Flow

1. **Session Injection (One-Time / Manual):**
    * User invokes `bot auth`.
    * **Camoufox** launches a visible, non-headless browser window. The user performs a manual login via Google/Email, resolving any necessary Captchas manually.
    * The script securely extracts session cookies (`uid`, `sid`, `xsrf`, etc.) and stores them locally in an encrypted vault (like `.env` or system keyring).
    * The browser process terminates.

2. **The Daily Loop (Headless / Automated):**
    * User (or a background chron job) runs `bot run`.
    * **curl-cffi** ingests the stored cookies and constructs a session rigorously impersonating `chrome120`.
    * The "Brain" assesses the **Daily Budget** (e.g., verifying limits like "only 45 follows permitted today").
    * The **Graph Traversal Strategy** executes organically until the budget threshold is hit.

---

## 3. Core Features & "Truth" Logic

Standard bot scripts often guess API behaviors or use deprecated REST endpoints. This system is driven by **Verified Ground Truth** based on exact, captured network heuristics from current (2026) Medium web client behavior.

### A. The "Follow" Logic (Critical UI Reality)

Standard automations fail by naively using hypothetical `UserFollow` mutations. Analysis of actual Medium feed mechanics reveals a different UI reality.

* **Action:** "Follow User" (from feed/articles)
* **Actual GraphQL Mutation:** `SubscribeNewsletterV3Mutation`
* **Logic:**
    1. Query the target user or feed to extract the `newsletterV3Id` (Critically **distinct** from their `userId`).
    2. Dispatch the mutation strictly including `shouldRecordConsent: false`.
    3. **Why:** This accurately replicates clicking the default "Follow" button inline on a user's content feed, which natively resolves to subscribing to the user's newsletter channel rather than a purely graph-based follow.

### B. The "Unfollow" Logic

* **Action:** "Unfollow User"
* **Actual GraphQL Mutation:** `UnfollowUserMutation`
* **Logic:**
    1. Unlike following (which is a subscription), breaking the connection definitively requires the full unfollow mutation using the target's explicit `userId`. 
    2. This effectively clears the `viewerEdge.isFollowing` state.

### C. Discovery & Warm-up (The "Human" Touch)

* **Source Operation:** `TopicLatestStorieQuery` (e.g., polling the `programming` tag).
* **Behavioral Sequence:**
    1. **Fetch:** Retrieve the latest stories associated with a chosen tag.
    2. **Read (Wait):** Induce an intentional `sleep()` randomized between `30s` and `90s` to emulate time-on-page and reading behavior.
    3. **Warm/Clap (Optional):** Dispatch `ClapMutation` containing randomized counts (1-5 claps) to organically warm the session and simulate content appreciation before immediately connecting.
    4. **Decide:** Parse the `creator` dictionary from the evaluated story to extract the `newsletterV3Id` and execute the follow/subscribe decision.

---

## 4. Data Model (SQLite Schema)

A deliberate, lightweight schema optimized for "Time-Travel" diffing (monitoring follower growth/loss) and strict rate-limiting compliance.

```sql
-- 1. The Registry: Who have we seen?
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    username TEXT,
    newsletter_id TEXT, -- Critical for the 'Subscribe' mutation flow
    bio TEXT,
    last_scraped_at DATETIME
);

-- 2. The Relationship State: Who do we follow?
CREATE TABLE relationships (
    user_id TEXT,
    state TEXT CHECK(state IN ('following', 'blocking', 'muted', 'none')),
    updated_at DATETIME,
    PRIMARY KEY (user_id)
);

-- 3. The Budget & Audit Log: What have we done?
-- This table rigorously enforces the "Daily Budget" and limits visibility.
CREATE TABLE action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT, -- e.g., 'subscribe', 'unsubscribe', 'clap'
    target_id TEXT,
    status TEXT, -- 'success', 'failed'
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 4. Snapshots: For "Growth" Reporting
CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY,
    date DATE,
    follower_count INTEGER,
    following_count INTEGER
);
```

---

## 5. Technical Implementation Specs

### The "Stealth" Request Builder

Every HTTP request must rigidly conform to real-world browser fingerprints to evade Cloudflare and Medium's internal API request heuristics.

```python
# Stealth Client Abstraction (curl-cffi)
from curl_cffi import requests

class StealthClient:
    def __init__(self, cookies: dict):
        self.session = requests.Session(
            impersonate="chrome120",  # <--- The Magic TLS/JA3 Key
            headers={
                "Origin": "https://medium.com",
                "Referer": "https://medium.com/", # Dynamic based on emulated UI path
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "apollographql-client-name": "lite",
                "apollographql-client-version": "main-2026-...", # Matches current web builds
            },
            cookies=cookies
        )

    def execute_graphql(self, operation_name, variables, query):
        # Implementation referencing exact payload structures 
        # documented in practical_capture_2026-02-21.json
        pass
```

### The "Daily Budget" Algorithm

Ensures the bot never acts erratically or identically to a recursive scraper.

```python
MAX_ACTIONS_PER_DAY = 50

def run_daily_cycle():
    # 1. Enforce Budget Constraints Strictly
    actions_today = db.query(
        "SELECT COUNT(*) FROM action_log WHERE timestamp > start_of_day"
    )
    
    if actions_today >= MAX_ACTIONS_PER_DAY:
        print("Budget exhausted. Sleeping until tomorrow.")
        return

    # 2. Execute Remaining Actions Safely
    remaining = MAX_ACTIONS_PER_DAY - actions_today
    perform_actions(limit=remaining)
```

---

## 6. Directory Structure

```text
/medium-stealth-bot
 ├── /captures              # Ground-truth JSONs from actual network tools (reference only)
 ├── /data                  # SQLite database file and .env vault files
 ├── /src
 │    ├── __init__.py
 │    ├── main.py           # Typer CLI application entry point
 │    ├── auth.py           # Camoufox hybrid login handler
 │    ├── client.py         # curl-cffi stealth network wrapper
 │    ├── database.py       # SQLite models, schemas & migrations
 │    ├── repository.py     # Database CRUD / Access Layer
 │    ├── operations.py     # Verified GraphQL definitions (The "Mutation" Dictionary)
 │    └── logic.py          # The "Brain" (Budgeting, Targeting, Organic Sleeping)
 ├── requirements.txt
 └── README.md
```

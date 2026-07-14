# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Hebrew RTL business-intelligence dashboard for **דנא ציוד אינסטלציה (DNA Tools)**, a B2B plumbing/water-infrastructure equipment store in Israel. Single-page app + two independent Cloud Run/Cloud Function backends, all on Firestore.

## Architecture

- **`index.html`** — the entire frontend. Vanilla JS/HTML/CSS, no build step, no framework, **one single `<script>` tag** for all ~5,800 lines of application logic. There is no bundler and no module system — everything is a global function/variable in that one script block.
- **`backend/competitors/main.py`** — the main backend. A single Python Cloud Run service (via `functions_framework`, entry point `competitors_api`) backing almost every feature: CRM, catalog/pricing, competitor price tracking, the "העוזרת שלי" AI assistant, Ad Lab (Meta ads analysis), Facebook publishing, WooCommerce sync, email bridge. Deployed at `https://dna-competitors-477673568731.europe-west1.run.app` via **continuous deployment from GitHub** (Cloud Build trigger on push to `main`, build context `/backend/competitors`) — a plain `git push` deploys it, no manual steps.
- **`backend/main.py`** — a separate, small Cloud Function (entry point `analytics`) that proxies Google Analytics 4 data. Independent of `backend/competitors`, deployed separately.
- **`scraper.py`** + **`.github/workflows/daily-scan.yml`** — a GitHub Action (weekly, Saturday) that scrapes dna-tools.co.il for new products into `products.json`, committed back to the repo by a bot. Unrelated to competitor-price scanning (that runs server-side in `backend/competitors`, also weekly/Saturday, gated by request User-Agent so manual scans stay unaffected).
- **Frontend hosting**: GitHub Pages, served directly from this repo (`avi593/dna-sales-app`). A `git push` to `main` deploys both the frontend (Pages) and the main backend (Cloud Build) — there is no separate frontend build/deploy step.

### Backend routing

`backend/competitors/main.py` is a hand-rolled REST router, not a framework: `competitors_api(request)` splits `request.path` into `parts`, treats `parts[0]` as the resource name and `parts[1]` as an item id, and dispatches via a long `if/elif resource == "...":` chain near the bottom of the file. To add an endpoint: write a handler function, add an `elif resource == "your-resource":` branch, return via the `_json(body, status)` helper (returns a `(dict, status, headers)` tuple).

Auth is optional and currently disabled (`API_KEY` env var, checked via `X-API-Key` header if set — unset in production). CORS is open. Firestore document field writes go through per-entity whitelist sets (e.g. `CUSTOMER_FIELDS`, `TASK_FIELDS`, `CATALOG_FIELDS`, `AD_SNAPSHOT_FIELDS`) filtered via a `_pick(data, allowed)` helper — there is no schema/ORM, these whitelists *are* the schema. Firestore collections in use: `customers`, `tasks`, `catalog`, `models`/`priceSources` (competitor price tracking), `competitors`/`trackedPages`/`changes` (legacy competitor-intel module), `assistantMessages`, `adSnapshots`/`adAnalyses`/`adCreativeReviews` (Ad Lab), `posts` (Facebook), `documents`, `processedEmails`, `config` (single `settings` doc holding all API keys/tokens server-side).

### Frontend structure

- Client-side state is a flat set of global `let`/`const` variables (e.g. `catalog`, `customers`, `crmTasks`, `adSnaps`) reloaded from the backend on demand — no state management library.
- Navigation is tab-based: sidebar `nav-*` buttons call `showView('name')` which toggles `.view` div visibility; each view has a matching `id="view-name"` and its own `load*()`/`render*()` function pair. Not every view has a nav button — some (`view-dashboard`, `view-market`, `view-prices`) were intentionally unlinked from nav but left in the DOM/code rather than deleted, reachable only from specific in-app actions.
- `compApi(path, method, body)` is the fetch wrapper used everywhere to call the main backend; base URL comes from `COMP_API_DEFAULT` (overridable via `localStorage['dna_comp_api']`).
- AI features (the assistant, Ad Lab's rules engine + Gemini review, creative analysis) run through a shared server-side helper `_call_gemini_structured(prompt, schema, image_b64=None, image_mime=None)` in the Python backend — Gemini calls always happen server-side with a key stored in Firestore `config`, never in the browser, and always request structured JSON output via `responseSchema`. Some older/legacy features (e.g. the general chat advisor, price-list PDF import) instead call Gemini directly from the browser using a user-supplied client-side key (`apiKey`, `dna_api_key` in localStorage) — don't assume all AI calls are server-side without checking.

## Critical workflow: syntax-checking `index.html`

Because the entire frontend is one `<script>` block, **a single JS syntax error breaks every button in the app**, and there is no build step or linter to catch it before deploy. Before committing any `index.html` change, validate the script block parses:

```bash
node -e '
const fs=require("fs"),vm=require("vm");
const html=fs.readFileSync("index.html","utf8");
const m=[...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
let bad=0; m.forEach((s,i)=>{ try{ new vm.Script(s[1]); }catch(e){ bad++; console.log("ERR: "+e.message);} });
console.log(bad? "SYNTAX FAIL":"SYNTAX OK");
'
```

For the Python backend, the equivalent check:

```bash
python -c "import ast; ast.parse(open('backend/competitors/main.py', encoding='utf-8').read()); print('PY SYNTAX OK')"
```

There is no test suite for either backend or frontend — these AST/parse checks are the only pre-deploy safety net, and Cloud Build's own deploy failure is otherwise the first real signal something broke.

## Deploying

```bash
git add <files>
git commit -m "..."
git push          # deploys frontend (GitHub Pages) AND backend/competitors (Cloud Build) automatically
```

Cloud Build for `backend/competitors` takes roughly 1–2 minutes after push; GitHub Pages a similar order of magnitude. `backend/main.py` (the GA4 function) is **not** wired to this same continuous-deployment trigger and is deployed separately/manually.

## Open tasks

- **Zero attributed purchases on active paid Meta campaigns.** Two live campaigns — "תנועות לאתר קידום מוצרים" (OUTCOME_TRAFFIC, started 2026-07-05) and "רימרקטינג - מבקרי אתר" (OUTCOME_SALES, started 2026-07-01) — have spent a combined **₪1,049** (₪266.69 + ₪782.44) through 2026-07-14 with real traffic (28K + 54K impressions, 3.5K + 2K link clicks) but **zero purchases/purchase value attributed on either**, per `/ads-campaigns-performance`. Likely connected to the Meta Pixel not being properly installed on dna-tools.co.il until 2026-07-12 (fixed this session via the "Meta for WooCommerce" plugin — disconnect/reconnect was required to actually get the Pixel base code injected, not just the Business Manager + catalog connection). Needs investigation in a separate conversation: confirm Purchase events are now firing correctly, check whether the "רימרקטינג" campaign's Custom Audience was ever populated (it should be built from real Pixel visitor data), and assess how much of the ₪1,049 was wasted spend during the tracking gap.

## Local dev server

`.claude/launch.json` defines a local static server (`python -m http.server 3000`) for previewing `index.html` — it does **not** proxy to either backend, so anything that calls `compApi()` hits the live Cloud Run backend even when previewing locally. There is no local emulation of Firestore or the Python backends.

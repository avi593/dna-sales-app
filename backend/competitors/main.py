"""
דנא ציוד אינסטלציה — Backend מודיעין מתחרים (שלב 1: שכבת נתונים + API)
Google Cloud Run function שמנהל מתחרים ודפי מעקב ב-Firestore.

Entry point: competitors_api
Runtime: Python 3.12

משתני סביבה:
  API_KEY        — אופציונלי. אם מוגדר, כל בקשה חייבת לכלול כותרת X-API-Key תואמת.
  ALLOWED_ORIGIN — אופציונלי. דומיין שמותר לו לקרוא (ברירת מחדל: *)

מבנה הנתונים (Firestore collections):
  competitors   { name, website, category, status, notes, createdAt, updatedAt }
  trackedPages  { competitorId, url, pageType, label, enabled, priceSelector,
                  renderMode, crawlFrequency, lastCrawledAt, lastSnapshotId,
                  nextCrawlAt, createdAt, updatedAt }
  changes       { competitorId, trackedPageId, type, oldValue, newValue,
                  severity, summary, detectedAt }   (לקריאה בלבד בשלב זה)

ניתוב (routing) לפי method + path:
  GET    /competitors                 → רשימת מתחרים
  POST   /competitors                 → יצירת מתחרה
  PUT    /competitors/{id}            → עדכון מתחרה
  DELETE /competitors/{id}            → מחיקת מתחרה (+ דפי המעקב שלו)
  GET    /pages?competitorId={id}     → דפי מעקב (אופציונלי מסונן למתחרה)
  POST   /pages                       → יצירת דף מעקב
  PUT    /pages/{id}                  → עדכון דף מעקב
  DELETE /pages/{id}                  → מחיקת דף מעקב
  GET    /changes?competitorId=&type= → פיד שינויים (ימולא בשלב 3)
"""

import os
import re
import time
import random
import base64
import json as _jsonlib
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, unquote
import functions_framework
import requests
from bs4 import BeautifulSoup
from google.cloud import firestore

API_KEY = os.environ.get("API_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

CORS = {
    "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-API-Key",
}

db = firestore.Client()

# ── שדות מותרים לכל ישות (whitelist — מונע כתיבת שדות זרים) ──
COMPETITOR_FIELDS = {"name", "website", "category", "status", "notes"}
CUSTOMER_FIELDS = {"name", "company", "phone", "email", "status", "notes", "lastContact",
                   "stage", "source", "industry", "companySize", "website",
                   "aiScore", "aiSummary", "nextAction", "interests", "lastOrderAt", "content"}
TASK_FIELDS = {"title", "customerId", "dueDate", "priority", "status", "notes",
                "parentTaskId", "isParent", "sequenceOrder", "progressPercent",
                "sourceMessageId", "needsReview", "category", "adSnapshotId"}
# מרכז ניתוח מודעות Meta: שורת snapshot = מדדי מודעה אחת לתקופה מסוימת (נשמר לפי תאריך להשוואות)
AD_SNAPSHOT_FIELDS = {
    "adName", "campaign", "adset", "status", "platform", "placement", "creativeType",
    "product", "landingUrl", "periodStart", "periodEnd",
    "spend", "impressions", "reach", "frequency", "cpm",
    "linkClicks", "lpViews", "ctrLink", "ctrAll", "cpcLink",
    "viewContent", "addToCart", "initCheckout", "purchases", "purchaseValue",
    "roas", "costPerPurchase", "qualityRank", "engagementRank", "convRank", "notes",
    "primaryText", "headline", "description", "cta",
}
DOCUMENT_FIELDS = {"customerId", "name", "type", "data"}
PAGE_FIELDS = {
    "competitorId", "url", "pageType", "label", "enabled",
    "priceSelector", "renderMode", "crawlFrequency",
}


def _doc_to_dict(doc):
    d = doc.to_dict() or {}
    d["id"] = doc.id
    return d


def _ts_to_iso(d):
    """ממיר שדות timestamp של Firestore למחרוזות ISO לשידור JSON."""
    for k, v in list(d.items()):
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
    return d


def _pick(data, allowed):
    return {k: v for k, v in (data or {}).items() if k in allowed}


def _json(body, status=200):
    return (body, status, {**CORS, "Content-Type": "application/json"})


# ═══════════════════════════ COMPETITORS ═══════════════════════════

def list_competitors():
    docs = db.collection("competitors").order_by(
        "createdAt", direction=firestore.Query.DESCENDING
    ).stream()
    return _json({"competitors": [_ts_to_iso(_doc_to_dict(d)) for d in docs]})


def create_competitor(data):
    fields = _pick(data, COMPETITOR_FIELDS)
    if not fields.get("name"):
        return _json({"error": "שדה 'name' הוא חובה"}, 400)
    fields.setdefault("status", "active")
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("competitors").document()
    ref.set(fields)
    return _json({"id": ref.id, **_ts_to_iso(_doc_to_dict(ref.get()))}, 201)


def update_competitor(cid, data):
    fields = _pick(data, COMPETITOR_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("competitors").document(cid)
    if not ref.get().exists:
        return _json({"error": "מתחרה לא נמצא"}, 404)
    ref.update(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())))


def delete_competitor(cid):
    ref = db.collection("competitors").document(cid)
    if not ref.get().exists:
        return _json({"error": "מתחרה לא נמצא"}, 404)
    # מחיקת דפי המעקב הקשורים (cascade) בקבוצות (batch)
    pages = db.collection("trackedPages").where("competitorId", "==", cid).stream()
    batch = db.batch()
    n = 0
    for p in pages:
        batch.delete(p.reference)
        n += 1
        if n % 400 == 0:
            batch.commit()
            batch = db.batch()
    batch.delete(ref)
    batch.commit()
    return _json({"deleted": cid, "pagesDeleted": n})


# ═══════════════════════════ TRACKED PAGES ═══════════════════════════

def list_pages(competitor_id=None):
    col = db.collection("trackedPages")
    query = col.where("competitorId", "==", competitor_id) if competitor_id else col
    docs = query.stream()
    pages = [_ts_to_iso(_doc_to_dict(d)) for d in docs]
    pages.sort(key=lambda p: p.get("createdAt") or "", reverse=True)
    return _json({"pages": pages})


def create_page(data):
    fields = _pick(data, PAGE_FIELDS)
    if not fields.get("competitorId"):
        return _json({"error": "שדה 'competitorId' הוא חובה"}, 400)
    if not fields.get("url"):
        return _json({"error": "שדה 'url' הוא חובה"}, 400)
    fields.setdefault("pageType", "other")
    fields.setdefault("enabled", True)
    fields.setdefault("renderMode", "http")
    fields.setdefault("crawlFrequency", "6h")
    fields["lastCrawledAt"] = None
    fields["lastSnapshotId"] = None
    fields["nextCrawlAt"] = firestore.SERVER_TIMESTAMP  # זכאי לסריקה מיידית
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("trackedPages").document()
    ref.set(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())), 201)


def update_page(pid, data):
    fields = _pick(data, PAGE_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("trackedPages").document(pid)
    if not ref.get().exists:
        return _json({"error": "דף מעקב לא נמצא"}, 404)
    ref.update(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())))


def delete_page(pid):
    ref = db.collection("trackedPages").document(pid)
    if not ref.get().exists:
        return _json({"error": "דף מעקב לא נמצא"}, 404)
    ref.delete()
    return _json({"deleted": pid})


# ═══════════════════════════ CHANGES (read-only כעת) ═══════════════════════════

def list_changes(competitor_id=None, change_type=None):
    col = db.collection("changes")
    query = col
    if competitor_id:
        query = query.where("competitorId", "==", competitor_id)
    if change_type:
        query = query.where("type", "==", change_type)
    docs = query.stream()
    changes = [_ts_to_iso(_doc_to_dict(d)) for d in docs]
    changes.sort(key=lambda c: c.get("detectedAt") or "", reverse=True)
    return _json({"changes": changes[:200]})


# ═══════════════════════════ PRICE TRACKING (דגמים) ═══════════════════════════
# מודל: כל "דגם" (model) הוא מוצר ספציפי עם הקישור והמחיר שלי, ומולו מתחרים (priceSources)
# שמוכרים את אותו דגם — כל אחד עם קישור ומחיר. הסורק מושך מחיר אמיתי מכל קישור.

MODEL_FIELDS = {"name", "myUrl", "myPrice", "myCost", "status"}
SOURCE_FIELDS = {"modelId", "name", "url"}

SCAN_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}

# מאגר דפדפנים אמיתיים — מסובבים אקראית כדי שהסריקה לא תיראה כמו אותו בוט בכל פעם.
# כל פרופיל עקבי (UA + client-hints תואמים) כדי לא ליצור חתימה חשודה.
_UA_PROFILES = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
     "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"', "Sec-Ch-Ua-Mobile": "?0", "Sec-Ch-Ua-Platform": '"Windows"'},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
     "Sec-Ch-Ua": '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"', "Sec-Ch-Ua-Mobile": "?0", "Sec-Ch-Ua-Platform": '"macOS"'},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
     "Sec-Ch-Ua": '"Microsoft Edge";v="124", "Chromium";v="124", "Not-A.Brand";v="99"', "Sec-Ch-Ua-Mobile": "?0", "Sec-Ch-Ua-Platform": '"Windows"'},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15"},
]
_BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Upgrade-Insecure-Requests": "1",
}

def _scan_headers():
    """כותרות בקשה עם דפדפן אקראי מהמאגר — מקשה על מתחרה לזהות את הסריקה."""
    h = dict(_BASE_HEADERS)
    h.update(random.choice(_UA_PROFILES))
    return h


def _to_price(val):
    """ממיר מחרוזת/מספר למחיר float, מטפל בפסיקי אלפים ובנקודה עשרונית."""
    try:
        s = re.sub(r"[^\d.,]", "", str(val))
        if not s:
            return None
        if "," in s and "." in s:
            s = s.replace(",", "")
        elif "," in s:
            parts = s.split(",")
            s = s.replace(",", "") if len(parts[-1]) == 3 else s.replace(",", ".")
        return round(float(s), 2)
    except Exception:
        return None


def extract_price(html):
    """מחלץ מחיר מדף HTML במספר שיטות נפוצות (JSON-LD, meta, itemprop, regex של ₪)."""
    soup = BeautifulSoup(html, "html.parser")

    # 1. JSON-LD (schema.org Product/Offer)
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = _jsonlib.loads(tag.string or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict):
                continue
            offers = it.get("offers")
            if isinstance(offers, list):
                offers = offers[0] if offers else None
            if isinstance(offers, dict):
                price = _to_price(offers.get("price") or offers.get("lowPrice"))
                if price:
                    return price

    # 2. meta tags נפוצים
    for attrs in ({"property": "product:price:amount"},
                  {"property": "og:price:amount"},
                  {"itemprop": "price"}):
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            price = _to_price(m["content"])
            if price:
                return price

    # 3. itemprop=price על כל אלמנט
    el = soup.find(attrs={"itemprop": "price"})
    if el:
        price = _to_price(el.get("content") or el.get_text())
        if price:
            return price

    # 4. JSON מוטמע: "price": <מספר> — תופס נתוני מוצר בתוך סקריפטים (מתעלם מאפסים)
    for mm in re.finditer(r'"price"\s*:\s*"?([\d.,]+)"?', html):
        price = _to_price(mm.group(1))
        if price:
            return price

    # 5. regex של ₪ / ש"ח / NIS ליד מספר
    text = soup.get_text(" ", strip=True)
    m = (re.search(r"(?:₪|ש\"?ח|NIS)\s*([\d.,]+)", text)
         or re.search(r"([\d.,]+)\s*(?:₪|ש\"?ח|NIS)", text))
    if m:
        return _to_price(m.group(1))

    return None


def fetch_price(url):
    try:
        r = requests.get(url, headers=_scan_headers(), timeout=20)
        if r.status_code != 200:
            return {"price": None, "status": r.status_code}
        # תיקון קידוד: כשהשרת לא מצהיר charset, requests מניח ISO-8859-1 ושובר את ₪.
        if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
            r.encoding = r.apparent_encoding or "utf-8"
        return {"price": extract_price(r.text), "status": 200}
    except Exception as e:
        return {"price": None, "status": "error", "error": str(e)}


def _sources_for(model_id):
    docs = db.collection("priceSources").where("modelId", "==", model_id).stream()
    out = [_ts_to_iso(_doc_to_dict(d)) for d in docs]
    out.sort(key=lambda s: s.get("createdAt") or "")
    return out


def list_models():
    docs = db.collection("models").order_by(
        "createdAt", direction=firestore.Query.DESCENDING
    ).stream()
    models = []
    for d in docs:
        m = _ts_to_iso(_doc_to_dict(d))
        m["competitors"] = _sources_for(d.id)
        models.append(m)
    return _json({"models": models})


def create_model(data):
    fields = _pick(data, MODEL_FIELDS)
    if not fields.get("name"):
        return _json({"error": "שדה 'name' הוא חובה"}, 400)
    fields.setdefault("status", "active")
    fields.setdefault("myPrice", None)
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("models").document()
    ref.set(fields)
    m = _ts_to_iso(_doc_to_dict(ref.get()))
    m["competitors"] = []
    return _json(m, 201)


def update_model(mid, data):
    fields = _pick(data, MODEL_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("models").document(mid)
    if not ref.get().exists:
        return _json({"error": "דגם לא נמצא"}, 404)
    ref.update(fields)
    m = _ts_to_iso(_doc_to_dict(ref.get()))
    m["competitors"] = _sources_for(mid)
    return _json(m)


def delete_model(mid):
    ref = db.collection("models").document(mid)
    if not ref.get().exists:
        return _json({"error": "דגם לא נמצא"}, 404)
    srcs = db.collection("priceSources").where("modelId", "==", mid).stream()
    batch = db.batch()
    n = 0
    for s in srcs:
        batch.delete(s.reference)
        n += 1
        if n % 400 == 0:
            batch.commit()
            batch = db.batch()
    batch.delete(ref)
    batch.commit()
    return _json({"deleted": mid, "competitorsDeleted": n})


def create_source(data):
    fields = _pick(data, SOURCE_FIELDS)
    if not fields.get("modelId"):
        return _json({"error": "שדה 'modelId' הוא חובה"}, 400)
    if not fields.get("url"):
        return _json({"error": "שדה 'url' הוא חובה"}, 400)
    if not fields.get("name"):
        fields["name"] = re.sub(r"^https?://(www\.)?", "", fields["url"]).split("/")[0]
    fields["lastPrice"] = None
    fields["lastScanAt"] = None
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("priceSources").document()
    ref.set(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())), 201)


def update_source(sid, data):
    fields = _pick(data, {"name", "url", "manualPrice"})   # manualPrice — מחיר ידני (גובר על הסריקה)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    ref = db.collection("priceSources").document(sid)
    if not ref.get().exists:
        return _json({"error": "מתחרה לא נמצא"}, 404)
    ref.update(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())))


def delete_source(sid):
    ref = db.collection("priceSources").document(sid)
    if not ref.get().exists:
        return _json({"error": "מתחרה לא נמצא"}, 404)
    ref.delete()
    return _json({"deleted": sid})


def scan_model(mid, delay=False):
    """סורק מחיר אמיתי: הדף שלי + כל המתחרים, מעדכן ב-Firestore ומחזיר את התוצאה.
    delay=True (סריקה אוטומטית): מערבב סדר ומוסיף השהיה אקראית בין בקשות — אנטי-זיהוי."""
    ref = db.collection("models").document(mid)
    snap = ref.get()
    if not snap.exists:
        return _json({"error": "דגם לא נמצא"}, 404)
    model = snap.to_dict()
    now = firestore.SERVER_TIMESTAMP
    result = {"modelId": mid}

    if model.get("myUrl"):
        my = fetch_price(model["myUrl"])
        ref.update({"myPrice": my["price"], "lastScanAt": now, "updatedAt": now})
        result["myPrice"] = my["price"]
        result["myStatus"] = my.get("status")

    comps = []
    sources = list(db.collection("priceSources").where("modelId", "==", mid).stream())
    if delay:
        random.shuffle(sources)
    for s in sources:
        sd = s.to_dict()
        res = fetch_price(sd.get("url", ""))
        s.reference.update({
            "lastPrice": res["price"], "lastScanAt": now, "lastStatus": res.get("status"),
        })
        comps.append({"id": s.id, "name": sd.get("name"),
                      "price": res["price"], "status": res.get("status")})
        if delay:
            time.sleep(random.uniform(0.4, 1.3))   # השהיה בין מתחרים
    result["competitors"] = comps
    return _json(result)


def scan_all(stealth=False):
    """סורק את כל הדגמים — מופעל ידנית ('סרוק הכל') ובתזמון שבועי (Cloud Scheduler).
    stealth=True (אוטומטי): מערבב סדר, jitter בתחילה והשהיות בין דגמים — אנטי-זיהוי.
    ידני (דפדפן): ללא השהיות — מהיר."""
    ids = [s.id for s in db.collection("models").stream()]
    if stealth:
        random.shuffle(ids)
        time.sleep(random.uniform(0, 10))        # jitter קל בתחילת הסריקה
    for mid in ids:
        try:
            scan_model(mid, delay=stealth)
        except Exception:
            pass
        if stealth:
            time.sleep(random.uniform(0.8, 2.2))  # השהיה אקראית בין דגמים
    return _json({"scanned": len(ids)})


# ═══════════════════════════ AUTO-DISCOVERY (גילוי מתחרים) ═══════════════════════════
# תהליך: שולפים דגמים מעמוד קטגוריה באתר של דנא → לכל דגם מחפשים מתחרים ב-SERP →
# מחזירים הצעות (ללא שמירה). רק לאחר אישור המשתמש נוצרים models + priceSources בפועל.

def _get_config(key, default=None):
    try:
        doc = db.collection("config").document("settings").get()
        if doc.exists:
            return (doc.to_dict() or {}).get(key, default)
    except Exception:
        pass
    return default


def set_config(data):
    fields = _pick(data, {"serpApiKey", "serpProvider", "ideogramKey", "metaToken",
                          "fbPageToken", "fbPageId", "igUserId", "adAccountId", "adsToken",
                          "leadIntakeKey", "wcApiBase", "wcKey", "wcSecret", "geminiKey",
                          "emailBridgeUrl", "emailBridgeKey"})
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    db.collection("config").document("settings").set(fields, merge=True)
    # מחזירים רק האם המפתחות מוגדרים — בלי לחשוף אותם
    return _json({"ok": True, "serpConfigured": bool(_get_config("serpApiKey")),
                  "ideogramConfigured": bool(_get_config("ideogramKey")),
                  "metaConfigured": bool(_get_config("metaToken")),
                  "fbConfigured": bool(_get_config("fbPageToken") and _get_config("fbPageId")),
                  "assistantConfigured": bool(_get_config("geminiKey"))})


def config_status():
    return _json({"serpConfigured": bool(_get_config("serpApiKey")),
                  "serpProvider": _get_config("serpProvider", "serpapi"),
                  "ideogramConfigured": bool(_get_config("ideogramKey")),
                  "metaConfigured": bool(_get_config("metaToken")),
                  "fbConfigured": bool(_get_config("fbPageToken") and _get_config("fbPageId")),
                  "igConfigured": bool(_get_config("fbPageToken") and _get_config("igUserId")),
                  "adsConfigured": bool(_get_config("adAccountId")),
                  "wcConfigured": bool(_get_config("wcApiBase") and _get_config("wcKey")),
                  "assistantConfigured": bool(_get_config("geminiKey")),
                  "emailConfigured": bool(_get_config("emailBridgeUrl") and _get_config("emailBridgeKey"))})


def _resolve_page_token(token, page):
    """ממיר אסימון משתמש / משתמש-מערכת לאסימון דף. אם כבר אסימון דף — מחזיר כפי שהוא."""
    try:
        r = requests.get(f"https://graph.facebook.com/v21.0/{page}",
                         params={"fields": "access_token", "access_token": token}, timeout=20)
        pt = (r.json() or {}).get("access_token")
        if pt:
            return pt
    except Exception:
        pass
    return token


def publish_facebook(data):
    """מפרסם פוסט לדף פייסבוק (טקסט, ותמונה אם סופקה כ-URL ציבורי). פרסום מאושר בלבד."""
    data = data or {}
    token = _get_config("fbPageToken")
    page = _get_config("fbPageId")
    if not token or not page:
        return _json({"error": "לא הוגדרו Page Token + Page ID של פייסבוק"}, 400)
    if data.get("test"):
        result = {"ok": False}
        # אילו דפים האסימון בכלל יכול לגשת אליהם — לאבחון מזהה/הרשאות
        try:
            ra = requests.get("https://graph.facebook.com/v21.0/me/accounts",
                              params={"fields": "name,id", "access_token": token, "limit": 50}, timeout=20)
            result["accessiblePages"] = [{"name": p.get("name"), "id": p.get("id")}
                                         for p in (ra.json() or {}).get("data", [])]
        except Exception:
            result["accessiblePages"] = []
        # בדיקת הדף הספציפי שהוזן
        pt = _resolve_page_token(token, page)
        try:
            r = requests.get(f"https://graph.facebook.com/v21.0/{page}",
                             params={"fields": "name,id", "access_token": pt}, timeout=20)
            d = r.json()
            if "error" in d:
                result["error"] = d["error"].get("message", "שגיאת פייסבוק")
            else:
                result.update({"ok": True, "page": d.get("name"), "pageId": d.get("id")})
        except Exception as e:
            result["error"] = str(e)
        return _json(result, 200)
    message = (data.get("message") or "").strip()
    image = (data.get("imageUrl") or "").strip()
    image_b64 = (data.get("imageBase64") or "").strip()
    if not message and not image and not image_b64:
        return _json({"error": "אין תוכן לפרסום"}, 400)
    # תזמון אופציונלי: scheduledTime = unix seconds (או ISO) — פייסבוק מפרסם לבד במועד
    sched = data.get("scheduledTime")
    sched_ts = None
    if sched:
        try:
            sched_ts = int(float(sched))
        except Exception:
            try:
                from datetime import datetime
                sched_ts = int(datetime.fromisoformat(str(sched).replace("Z", "+00:00")).timestamp())
            except Exception:
                return _json({"error": "scheduledTime לא תקין (unix seconds או ISO)"}, 400)
        now = int(time.time())
        if sched_ts < now + 600:
            return _json({"error": "זמן התזמון חייב להיות לפחות 10 דקות בעתיד"}, 400)
        if sched_ts > now + 60 * 60 * 24 * 180:
            return _json({"error": "זמן התזמון חורג מ-6 חודשים"}, 400)
    token = _resolve_page_token(token, page)
    try:
        if sched_ts:
            # פוסט מתוזמן: מעלים תמונה כלא-מפורסמת, ואז יוצרים פוסט-פיד מתוזמן עם המדיה המצורפת
            media_fbid = None
            if image_b64:
                if "," in image_b64:
                    image_b64 = image_b64.split(",", 1)[1]
                try:
                    img_bytes = base64.b64decode(image_b64)
                except Exception:
                    return _json({"error": "תמונה לא תקינה"}, 400)
                ru = requests.post(f"https://graph.facebook.com/v21.0/{page}/photos",
                                   data={"published": "false", "access_token": token},
                                   files={"source": ("image.jpg", img_bytes, "image/jpeg")}, timeout=60)
                du = ru.json()
                if "error" in du:
                    return _json({"error": du["error"].get("message", "שגיאת פייסבוק"), "raw": du["error"]}, 502)
                media_fbid = du.get("id")
            elif image:
                ru = requests.post(f"https://graph.facebook.com/v21.0/{page}/photos",
                                   data={"published": "false", "url": image, "access_token": token}, timeout=30)
                du = ru.json()
                if "error" in du:
                    return _json({"error": du["error"].get("message", "שגיאת פייסבוק"), "raw": du["error"]}, 502)
                media_fbid = du.get("id")
            feed_data = {"access_token": token, "published": "false", "scheduled_publish_time": sched_ts}
            if message:
                feed_data["message"] = message
            if media_fbid:
                feed_data["attached_media[0]"] = _jsonlib.dumps({"media_fbid": media_fbid})
            r = requests.post(f"https://graph.facebook.com/v21.0/{page}/feed", data=feed_data, timeout=30)
        elif image_b64:
            # תמונה שהועלתה מהמחשב (data URL / base64) — מעלה כקובץ לדף
            if "," in image_b64:
                image_b64 = image_b64.split(",", 1)[1]
            try:
                img_bytes = base64.b64decode(image_b64)
            except Exception:
                return _json({"error": "תמונה לא תקינה"}, 400)
            r = requests.post(f"https://graph.facebook.com/v21.0/{page}/photos",
                              data={"caption": message, "access_token": token},
                              files={"source": ("image.jpg", img_bytes, "image/jpeg")}, timeout=60)
        elif image:
            r = requests.post(f"https://graph.facebook.com/v21.0/{page}/photos",
                              data={"url": image, "caption": message, "access_token": token}, timeout=30)
        else:
            r = requests.post(f"https://graph.facebook.com/v21.0/{page}/feed",
                              data={"message": message, "access_token": token}, timeout=30)
        d = r.json()
        if "error" in d:
            return _json({"error": d["error"].get("message", "שגיאת פייסבוק"), "raw": d["error"]}, 502)
        post_id = d.get("post_id") or d.get("id")
        if post_id:  # שומר את הפוסט לדוחות וללמידה
            try:
                db.collection("posts").document(str(post_id)).set({
                    "postId": str(post_id),
                    "message": (message or "")[:500],
                    "hasImage": bool(image or image_b64),
                    "scheduledTime": sched_ts,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                    "likes": 0, "comments": 0, "shares": 0, "reactions": 0,
                }, merge=True)
            except Exception:
                pass
        return _json({"ok": True, "postId": post_id, "scheduled": bool(sched_ts), "scheduledTime": sched_ts,
                      "url": (None if sched_ts else (f"https://www.facebook.com/{post_id}" if post_id else None))})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def list_customers():
    docs = db.collection("customers").order_by("createdAt", direction=firestore.Query.DESCENDING).stream()
    return _json({"customers": [_ts_to_iso(_doc_to_dict(d)) for d in docs]})


def create_customer(data):
    fields = _pick(data, CUSTOMER_FIELDS)
    if not fields.get("name"):
        return _json({"error": "שדה 'name' הוא חובה"}, 400)
    fields.setdefault("status", "ליד")
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("customers").document()
    ref.set(fields)
    return _json({"id": ref.id, **_ts_to_iso(_doc_to_dict(ref.get()))}, 201)


def update_customer(cid, data):
    fields = _pick(data, CUSTOMER_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("customers").document(cid)
    if not ref.get().exists:
        return _json({"error": "לקוח לא נמצא"}, 404)
    ref.update(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())))


def delete_customer(cid):
    ref = db.collection("customers").document(cid)
    if not ref.get().exists:
        return _json({"error": "לקוח לא נמצא"}, 404)
    ref.delete()
    return _json({"deleted": cid})


def delete_all_customers():
    docs = list(db.collection("customers").stream())
    batch = db.batch()
    n = 0
    for d in docs:
        batch.delete(d.reference)
        n += 1
        if n % 400 == 0:
            batch.commit()
            batch = db.batch()
    if n % 400 != 0:
        batch.commit()
    return _json({"deleted": n})


def list_documents(customer_id):
    if not customer_id:
        return _json({"documents": []})
    docs = db.collection("documents").where("customerId", "==", customer_id).stream()
    out = []
    for d in docs:
        out.append(_ts_to_iso(_doc_to_dict(d)))
    return _json({"documents": out})


def create_document(data):
    fields = _pick(data, DOCUMENT_FIELDS)
    if not fields.get("customerId") or not fields.get("data"):
        return _json({"error": "חסר customerId או קובץ"}, 400)
    if len(fields.get("data") or "") > 1_400_000:
        return _json({"error": "הקובץ גדול מדי (מקסימום ~720KB)"}, 400)
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("documents").document()
    ref.set(fields)
    return _json({"id": ref.id}, 201)


def delete_document(did):
    ref = db.collection("documents").document(did)
    if not ref.get().exists:
        return _json({"error": "מסמך לא נמצא"}, 404)
    ref.delete()
    return _json({"deleted": did})


def list_tasks(customer_id=None):
    if customer_id:
        docs = db.collection("tasks").where("customerId", "==", customer_id).stream()
    else:
        docs = db.collection("tasks").order_by("createdAt", direction=firestore.Query.DESCENDING).stream()
    return _json({"tasks": [_ts_to_iso(_doc_to_dict(d)) for d in docs]})


def create_task(data):
    fields = _pick(data, TASK_FIELDS)
    if not fields.get("title"):
        return _json({"error": "שדה 'title' הוא חובה"}, 400)
    fields.setdefault("status", "פתוח")
    fields.setdefault("priority", "רגיל")
    fields["taskNumber"] = _next_task_number()
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("tasks").document()
    ref.set(fields)
    return _json({"id": ref.id, **_ts_to_iso(_doc_to_dict(ref.get()))}, 201)


def update_task(tid, data):
    fields = _pick(data, TASK_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("tasks").document(tid)
    if not ref.get().exists:
        return _json({"error": "משימה לא נמצאה"}, 404)
    ref.update(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())))


def delete_task(tid):
    ref = db.collection("tasks").document(tid)
    if not ref.get().exists:
        return _json({"error": "משימה לא נמצאה"}, 404)
    ref.delete()
    return _json({"deleted": tid})


# ═══════════════════════════ ASSISTANT — "העוזרת שלי" (AviOS Phase A) ═══════════════════════════
# עקרון-על: כל הודעה נשמרת גולמית לפני כל עיבוד. ה-AI אינו כותב ל-DB ישירות —
# מחזיר JSON מובנה בלבד; השרת מאמת ומחליט אילו רשומות ליצור, לפי סף ביטחון (confidence gate).

ASSISTANT_SCHEMA = {
    "type": "object",
    "properties": {
        "intent": {"type": "string"},
        "is_query": {"type": "boolean"},
        "confidence": {"type": "number"},
        "needs_user_confirmation": {"type": "boolean"},
        "clarifying_question": {"type": "string"},
        "referenced_task_number": {"type": "number"},
        "update": {
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "due_date": {"type": "string"},
                "notes_append": {"type": "string"},
            },
        },
        "entities": {
            "type": "object",
            "properties": {
                "customer_name": {"type": "string"},
                "task_title": {"type": "string"},
                "due_date": {"type": "string"},
            },
        },
        "summary": {"type": "string"},
    },
}

ASSISTANT_SYS = """את/ה "העוזרת שלי" — עוזרת AI אישית לאבי, בעל דנא ציוד אינסטלציה (עסק B2B לציוד אינסטלציה בישראל).
המשתמש כותב הודעות חופשיות בעברית, ואת/ה מחלצת מהן כוונה, ומחזירה JSON בלבד לפי הסכימה שסופקה.
תאריך היום: __TODAY__ (שעון ישראל).

לכל משימה יש מספר קבוע (#). רשימת המשימות הפתוחות הקיימות כרגע מסופקת למטה — זו המקור היחיד לאמת לגבי אילו מספרים קיימים.

כללים מחייבים:
- referenced_task_number: אם ההודעה מתייחסת למשימה קיימת לפי מספר — למשל "משימה 3", "עדכן את 5", "3 — נסגר", או אפילו הודעה שהיא רק מספר בודד — התאימי לרשימת המשימות הפתוחות שסופקה למטה וקבעי את המספר. אם המספר שהוזכר לא מופיע ברשימה, עדיין קבעי אותו כאן (כדי שהמערכת תוכל להודיע שלא נמצא) — אל תמציאי משימה חלופית.
- אם נקבע referenced_task_number: אל תמלאי entities/task_title (זו לא יצירת משימה חדשה). אם המשתמש ציין מה לשנות (למשל "נסגר"/"הושלם"/"בוצע" → update.status="בוצע"; "תפתחי מחדש" → update.status="פתוח"; תאריך חדש → update.due_date; הערה → update.notes_append) — מלאי רק את מה שצוין במפורש, בלי להמציא. אם המשתמש רק שאל על המשימה בלי לבקש שינוי (למשל "מה קורה עם משימה 3") — השאירי update ריק וסמני is_query=true.
- is_query: סמני true אם ההודעה היא שאלה או בקשת מידע על מצב קיים — למשל "מה המשימות הפתוחות שלי", "מה חשוב לי עכשיו", "מה קורה עם ההזמנה של X" — ולא הוראה ליצור משהו חדש. אחרת false. שאלה כזו לעולם לא יוצרת משימה חדשה, רק מציגה מידע קיים.
- אם is_query=true (וללא referenced_task_number) — אין צורך למלא entities/task_title; summary יכול להיות ריק (המערכת עצמה תשלוף ותציג את המידע).
- אם is_query=false וללא referenced_task_number: לעולם אל תמציאי או תנחשי מידע חסר. אם משהו לא ברור — מי האדם, האם הוא לקוח או ספק, על מה בדיוק מדובר, או שיש כמה פירושים אפשריים — הורידי confidence מתחת ל-0.65 ונסחי clarifying_question ממוקדת וקצרה בעברית.
- confidence מעל 0.85 = ברור לגמרי, אין צורך באישור. 0.65–0.85 = סביר אך כדאי לוודא (סמני needs_user_confirmation=true אם יש ספק קל). מתחת ל-0.65 = לא ברור, יש לשאול (needs_user_confirmation=true).
- entities.due_date / update.due_date: אם המשתמש ציין תאריך יחסי ("מחר", "יום חמישי", "בעוד שבוע") — חשבי תאריך מדויק בפורמט YYYY-MM-DD לפי תאריך היום שסופק למעלה. אם לא צוין תאריך כלל — השאירי ריק.
- entities.task_title: ניסוח קצר, ברור ומעשי של המשימה (לא ציטוט מילולי של ההודעה).
- entities.customer_name: רק אם שם אדם/לקוח מפורש הוזכר; אחרת השאירי ריק.
- summary: משפט אחד קצר בעברית שמסכם מה הבנת — ישמש כתשובת הצ'אט למשתמש (רק כשis_query=false), אז שיהיה טבעי וידידותי.
- אל תחזירי שום טקסט מחוץ ל-JSON."""


def _call_gemini_structured(prompt, schema, image_b64=None, image_mime=None):
    """קריאת Gemini בצד-שרת עם פלט JSON מובנה. מפתח נשמר ב-Firestore config (geminiKey) — לא נחשף ללקוח.
    image_b64/image_mime אופציונליים — ניתוח מולטימודאלי (למשל תמונת קריאייטיב)."""
    key = _get_config("geminiKey")
    if not key:
        return None, "לא הוגדר מפתח Gemini (יש להגדיר דרך ⚙️ הגדרות בעוזרת שלי)"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={key}"
    parts = [{"text": prompt}]
    if image_b64:
        parts.insert(0, {"inline_data": {"mime_type": image_mime or "image/jpeg", "data": image_b64}})
    body = {
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": {"temperature": 0, "responseMimeType": "application/json", "responseSchema": schema},
    }
    last_err = None
    for attempt in range(1, 4):
        try:
            r = requests.post(url, json=body, timeout=(45 if image_b64 else 30))
            d = r.json()
            if r.status_code == 200:
                text = d["candidates"][0]["content"]["parts"][0]["text"]
                return _jsonlib.loads(text), None
            msg = (d.get("error") or {}).get("message", "")
            last_err = msg or f"שגיאת Gemini ({r.status_code})"
            if r.status_code in (429, 503) or re.search(r"overload|high demand|try again", msg, re.I):
                time.sleep(1.5 * attempt)
                continue
            return None, last_err
        except Exception as e:
            last_err = str(e)
            time.sleep(1.0)
    return None, last_err or "נכשל אחרי מספר ניסיונות"


def _find_customer_by_name(name):
    """התאמה חד-משמעית בלבד — שם מדויק (לא תלוי-רישיות). כמה התאמות או אפס → None (לא מנחשים)."""
    name = (name or "").strip()
    if not name:
        return None
    matches = [d for d in db.collection("customers").stream()
               if ((d.to_dict() or {}).get("name") or "").strip().lower() == name.lower()]
    return matches[0] if len(matches) == 1 else None


def _next_task_number():
    """מספר משימה רץ, ייחודי לכל משימה (ידנית או מהעוזרת) — כדי שאפשר יהיה להתייחס למשימה לפי מספר קצר.
    בריצה הראשונה גם משייך מספרים למשימות ישנות שנוצרו לפני התכונה, לפי סדר יצירה."""
    cfg_ref = db.collection("config").document("settings")
    snap = cfg_ref.get()
    cur = (snap.to_dict() or {}).get("taskCounter") if snap.exists else None
    if cur is None:
        legacy = [d for d in db.collection("tasks").stream() if not (d.to_dict() or {}).get("taskNumber")]
        legacy.sort(key=lambda d: (d.to_dict() or {}).get("createdAt") or 0)
        cur = 0
        for d in legacy:
            cur += 1
            d.reference.update({"taskNumber": cur})
    nxt = cur + 1
    cfg_ref.set({"taskCounter": nxt}, merge=True)
    return nxt


def _list_open_tasks(limit=10):
    """משימות פתוחות, ממוינות לפי תאריך יעד (עם תאריך קודם; בלי תאריך בסוף). קריאה בלבד — לא נוגעת ב-DB."""
    docs = list(db.collection("tasks").where("status", "==", "פתוח").stream())
    docs.sort(key=lambda d: (not (d.to_dict() or {}).get("dueDate"), (d.to_dict() or {}).get("dueDate") or ""))
    return docs[:limit]


def _open_tasks_reply():
    tasks = _list_open_tasks()
    if not tasks:
        return "אין לך כרגע משימות פתוחות. 🎉"
    lines = []
    for d in tasks:
        t = d.to_dict() or {}
        title = t.get("title") or "(ללא כותרת)"
        due = t.get("dueDate")
        num = t.get("taskNumber")
        lines.append("• " + (f"#{num} " if num else "") + title + (" — עד " + due if due else ""))
    return f"יש לך {len(tasks)} משימות פתוחות:\n" + "\n".join(lines)


def list_assistant_messages():
    docs = db.collection("assistantMessages").order_by(
        "createdAt", direction=firestore.Query.DESCENDING).limit(60).stream()
    items = [_ts_to_iso(_doc_to_dict(d)) for d in docs]
    items.reverse()  # ישן→חדש, לתצוגת צ'אט כרונולוגית
    return _json({"messages": items})


def assistant_process(data):
    text = (data.get("message") or "").strip()
    if not text:
        return _json({"error": "אין הודעה"}, 400)

    # עקרון-על: שומרים את ההודעה הגולמית לפני כל עיבוד — תמיד, גם אם ה-AI ייכשל
    msg_ref = db.collection("assistantMessages").document()
    msg_ref.set({"text": text, "role": "user", "createdAt": firestore.SERVER_TIMESTAMP, "processed": False})

    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("Asia/Jerusalem")).strftime("%Y-%m-%d")
    except Exception:
        today = datetime.utcnow().strftime("%Y-%m-%d")

    open_tasks = _list_open_tasks(30)
    if open_tasks:
        lines = []
        for d in open_tasks:
            t = d.to_dict() or {}
            num = t.get("taskNumber")
            if not num:
                continue
            lines.append(f"#{num}: {t.get('title','')}" + (f" (עד {t['dueDate']})" if t.get("dueDate") else ""))
        tasks_context = "המשימות הפתוחות הקיימות כרגע:\n" + "\n".join(lines) if lines else "אין כרגע משימות פתוחות עם מספר."
    else:
        tasks_context = "אין כרגע משימות פתוחות."

    prompt = ASSISTANT_SYS.replace("__TODAY__", today) + "\n\n" + tasks_context + "\n\nהודעת המשתמש:\n" + text
    result, err = _call_gemini_structured(prompt, ASSISTANT_SCHEMA)

    if err or not result:
        reply = "לא הצלחתי לעבד את ההודעה כרגע (" + (err or "שגיאה") + "). ההודעה נשמרה — אפשר לנסות שוב."
        msg_ref.update({"reply": reply, "error": err, "processed": True})
        return _json({"messageId": msg_ref.id, "reply": reply, "createdTasks": [], "needsClarification": False})

    # אזכור משימה קיימת לפי מספר — עדכון או הצגת פרטים. לעולם לא יוצר משימה חדשה.
    ref_num = result.get("referenced_task_number")
    if ref_num:
        ref_num = int(ref_num)
        task_doc = next(iter(db.collection("tasks").where("taskNumber", "==", ref_num).limit(1).stream()), None)
        if not task_doc:
            reply = f"לא מצאתי משימה מספר {ref_num}. אפשר לבדוק את המספר?"
            msg_ref.update({"reply": reply, "aiResult": result, "processed": True, "needsClarification": True})
            return _json({"messageId": msg_ref.id, "reply": reply, "createdTasks": [], "needsClarification": True})

        t = task_doc.to_dict() or {}
        upd = result.get("update") or {}
        changes = {}
        if (upd.get("status") or "").strip():
            changes["status"] = upd["status"].strip()
        if (upd.get("due_date") or "").strip():
            changes["dueDate"] = upd["due_date"].strip()
        if (upd.get("notes_append") or "").strip():
            cur_notes = t.get("notes") or ""
            changes["notes"] = (cur_notes + "\n" if cur_notes else "") + upd["notes_append"].strip()

        if not changes:
            # אין בקשת שינוי — רק הצגת פרטי המשימה
            reply = (f"משימה #{ref_num}: {t.get('title','(ללא כותרת)')}"
                     + (f" — עד {t['dueDate']}" if t.get("dueDate") else "")
                     + f" — סטטוס: {t.get('status','?')}")
            msg_ref.update({"reply": reply, "aiResult": result, "processed": True, "isQuery": True})
            return _json({"messageId": msg_ref.id, "reply": reply, "createdTasks": [],
                          "needsClarification": False, "isQuery": True})

        changes["updatedAt"] = firestore.SERVER_TIMESTAMP
        task_doc.reference.update(changes)
        reply = f'עודכן ✅ משימה #{ref_num} "{t.get("title","")}"'
        if "status" in changes:
            reply += f" — סטטוס: {changes['status']}"
        if "dueDate" in changes:
            reply += f" — תאריך יעד: {changes['dueDate']}"
        msg_ref.update({"reply": reply, "aiResult": result, "processed": True, "updatedTaskId": task_doc.id})
        return _json({"messageId": msg_ref.id, "reply": reply, "createdTasks": [], "needsClarification": False,
                      "updatedTask": {"id": task_doc.id, "number": ref_num, "title": t.get("title")}})

    # שאלת מידע (קריאה בלבד) — לעולם לא יוצרת רשומה, רק שולפת ומציגה מצב קיים
    if result.get("is_query"):
        reply = _open_tasks_reply()
        msg_ref.update({"reply": reply, "aiResult": result, "processed": True, "isQuery": True})
        return _json({"messageId": msg_ref.id, "reply": reply, "createdTasks": [],
                      "needsClarification": False, "isQuery": True})

    confidence = float(result.get("confidence") or 0)
    entities = result.get("entities") or {}
    clarifying = (result.get("clarifying_question") or "").strip()
    summary = (result.get("summary") or "").strip()

    if confidence < 0.65 or result.get("needs_user_confirmation"):
        reply = clarifying or summary or "אפשר לפרט קצת יותר? לא הבנתי מספיק כדי לפתוח משימה."
        msg_ref.update({"reply": reply, "aiResult": result, "processed": True, "needsClarification": True})
        return _json({"messageId": msg_ref.id, "reply": reply, "createdTasks": [], "needsClarification": True})

    # ביטחון מספיק — יוצרים משימה. 0.65–0.85 מסומן לבדיקה (needsReview), לא חוסם.
    needs_review = confidence < 0.85
    customer = _find_customer_by_name(entities.get("customer_name"))
    task_fields = {
        "title": (entities.get("task_title") or summary or text[:80]).strip(),
        "status": "פתוח", "priority": "רגיל",
        "notes": 'מקור (עוזרת AI): "' + text + '"',
        "sourceMessageId": msg_ref.id,
        "needsReview": needs_review,
        "taskNumber": _next_task_number(),
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }
    due = (entities.get("due_date") or "").strip()
    if due:
        task_fields["dueDate"] = due
    if customer:
        task_fields["customerId"] = customer.id
    task_ref = db.collection("tasks").document()
    task_ref.set(task_fields)
    created = [{"id": task_ref.id, "title": task_fields["title"], "dueDate": task_fields.get("dueDate"),
                "number": task_fields["taskNumber"],
                "customerName": (customer.to_dict() or {}).get("name") if customer else None}]

    num_tag = f'#{task_fields["taskNumber"]} '
    reply = ("נקלט ✅ " + summary) if summary else ("נקלט ✅ יצרתי משימה " + num_tag + task_fields["title"])
    if summary:
        reply += f" ({num_tag.strip()})"
    if needs_review:
        reply += " (מסומן לבדיקה — לא הייתי בטוחה ב-100%)"

    msg_ref.update({"reply": reply, "aiResult": result, "processed": True,
                    "createdTaskIds": [t["id"] for t in created]})
    return _json({"messageId": msg_ref.id, "reply": reply, "createdTasks": created,
                  "needsClarification": False, "needsReview": needs_review})


# ── מיילים → משימות: גשר Apps Script בחשבון הגוגל של המשתמש (קריאה בלבד, מוגן במפתח סודי) ──

def email_inbox(args):
    """שולף מיילים אחרונים דרך גשר ה-Apps Script ומסמן אילו כבר הפכו למשימות."""
    bridge = _get_config("emailBridgeUrl")
    key = _get_config("emailBridgeKey")
    if not bridge or not key:
        return _json({"configured": False, "emails": []})
    try:
        params = {"key": key}
        if args.get("q"):
            params["q"] = args.get("q")
        r = requests.get(bridge, params=params, timeout=30)
        try:
            d = r.json()
        except Exception:
            # הגשר החזיר משהו שאינו JSON (בד"כ דף HTML של גוגל) — נחזיר רמז לאבחון
            body = (r.text or "")[:200]
            hint = "נראה כמו דף התחברות/שגיאה של גוגל — בדוק שהפריסה היא 'אפליקציית אינטרנט' עם גישה ל'כל אחד', ושהכתובת היא ה-/exec מהפריסה האחרונה" \
                if "<html" in body.lower() or "<!doctype" in body.lower() else ""
            return _json({"configured": True, "emails": [],
                          "error": f"הגשר החזיר תשובה לא תקינה (סטטוס {r.status_code}). {hint}",
                          "raw": body}, 502)
    except Exception as e:
        return _json({"configured": True, "error": "הגשר לא הגיב: " + str(e), "emails": []}, 502)
    if d.get("error"):
        return _json({"configured": True, "error": "שגיאת גשר: " + str(d["error"]), "emails": []}, 502)
    emails = d.get("emails") or []
    # סימון מיילים שכבר טופלו (נוצרה מהם משימה)
    for em in emails:
        try:
            doc = db.collection("processedEmails").document(str(em.get("id"))).get()
            if doc.exists:
                pd = doc.to_dict() or {}
                em["processed"] = True
                em["taskNumbers"] = pd.get("taskNumbers") or []
            else:
                em["processed"] = False
        except Exception:
            em["processed"] = False
    return _json({"configured": True, "emails": emails})


def email_task(data):
    """יוצר משימה ממייל — דרך אותו מנוע של העוזרת (חילוץ AI + מספור + שמירת מקור)."""
    em = data or {}
    email_id = str(em.get("id") or "").strip()
    if not email_id:
        return _json({"error": "חסר מזהה מייל"}, 400)
    existing = db.collection("processedEmails").document(email_id).get()
    if existing.exists:
        pd = existing.to_dict() or {}
        return _json({"already": True, "reply": "כבר נוצרה משימה מהמייל הזה (#"
                      + ",".join(str(n) for n in (pd.get("taskNumbers") or [])) + ")"})
    msg = ("התקבל מייל מ־" + (em.get("from") or "לא ידוע")
           + (" בתאריך " + em.get("date") if em.get("date") else "")
           + '. נושא: "' + (em.get("subject") or "") + '".\n'
           + "תוכן המייל:\n" + (em.get("snippet") or "")
           + "\n\nצרי משימה מתאימה לטיפול בפנייה הזו (אם מבוקשת הצעת מחיר — משימה להכין ולשלוח הצעת מחיר).")
    body, status, _hdrs = assistant_process({"message": msg})
    created = body.get("createdTasks") or []
    if created:
        db.collection("processedEmails").document(email_id).set({
            "emailId": email_id, "from": em.get("from"), "subject": em.get("subject"),
            "taskIds": [t.get("id") for t in created],
            "taskNumbers": [t.get("number") for t in created if t.get("number")],
            "createdAt": firestore.SERVER_TIMESTAMP,
        })
    return _json(body, status)


# ═══════════════════════════ AD LAB — מרכז ניתוח מודעות Meta ═══════════════════════════

def _act_val(rows, *types):
    """שולף ערך מתוך מערך actions/action_values של Graph API לפי סוגי פעולה (הראשון שנמצא)."""
    for t in types:
        for a in (rows or []):
            if a.get("action_type") == t:
                try:
                    return float(a.get("value"))
                except Exception:
                    return None
    return None


def ads_sync(data):
    """מושך את ביצועי המודעות מ-Meta Marketing API (level=ad) ושומר כ-adSnapshots.
    Upsert לפי (שם מודעה + תקופה) — מזהה מסמך דטרמיניסטי, בלי כפילויות בריצות חוזרות."""
    import hashlib
    data = data or {}
    acct = (_get_config("adAccountId") or "").strip()
    token = (_get_config("adsToken") or _get_config("fbPageToken") or "").strip()
    if not acct:
        return _json({"error": "לא הוגדר מזהה חשבון מודעות"}, 400)
    if not token:
        return _json({"error": "לא הוגדר טוקן מודעות (ads_read)"}, 400)
    acct = acct if acct.startswith("act_") else "act_" + acct
    preset = data.get("datePreset") or "last_7d"

    # סטטוסים חיים של מודעות (קריאה נפרדת; לא קריטית — נמשיך גם אם תיכשל)
    status_map = {}
    try:
        rs = requests.get(f"https://graph.facebook.com/v21.0/{acct}/ads",
                          params={"fields": "name,effective_status", "limit": 200, "access_token": token}, timeout=30)
        for ad in (rs.json().get("data") or []):
            status_map[ad.get("name")] = ad.get("effective_status")
    except Exception:
        pass

    fields = ("ad_name,adset_name,campaign_name,spend,impressions,reach,frequency,cpm,"
              "inline_link_clicks,inline_link_click_ctr,cost_per_inline_link_click,ctr,"
              "actions,action_values,purchase_roas,website_purchase_roas,"
              "quality_ranking,engagement_rate_ranking,conversion_rate_ranking")
    url = f"https://graph.facebook.com/v21.0/{acct}/insights"
    params = {"fields": fields, "level": "ad", "date_preset": preset, "limit": 100, "access_token": token}
    rows, pages = [], 0
    try:
        while url and pages < 4:
            r = requests.get(url, params=params, timeout=45)
            j = r.json()
            if "error" in j:
                return _json({"error": j["error"].get("message", "שגיאת Meta"), "raw": j["error"]}, 502)
            rows += j.get("data") or []
            url = ((j.get("paging") or {}).get("next"))
            params = {}  # ה-next כבר כולל את הפרמטרים
            pages += 1
    except Exception as e:
        return _json({"error": str(e)}, 500)

    synced = 0
    for row in rows:
        name = row.get("ad_name")
        if not name:
            continue
        acts, vals = row.get("actions"), row.get("action_values")
        roas = None
        for k in ("purchase_roas", "website_purchase_roas"):
            arr = row.get(k)
            if arr:
                try:
                    roas = float(arr[0].get("value"))
                    break
                except Exception:
                    pass
        snap = {
            "adName": name, "adset": row.get("adset_name"), "campaign": row.get("campaign_name"),
            "status": status_map.get(name),
            "periodStart": row.get("date_start"), "periodEnd": row.get("date_stop"),
            "spend": row.get("spend"), "impressions": row.get("impressions"), "reach": row.get("reach"),
            "frequency": row.get("frequency"), "cpm": row.get("cpm"),
            "linkClicks": row.get("inline_link_clicks"), "ctrLink": row.get("inline_link_click_ctr"),
            "cpcLink": row.get("cost_per_inline_link_click"), "ctrAll": row.get("ctr"),
            "lpViews": _act_val(acts, "landing_page_view"),
            "viewContent": _act_val(acts, "omni_view_content", "view_content"),
            "addToCart": _act_val(acts, "omni_add_to_cart", "add_to_cart"),
            "initCheckout": _act_val(acts, "omni_initiated_checkout", "initiate_checkout"),
            "purchases": _act_val(acts, "omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase"),
            "purchaseValue": _act_val(vals, "omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase"),
            "roas": roas,
            "qualityRank": row.get("quality_ranking"), "engagementRank": row.get("engagement_rate_ranking"),
            "convRank": row.get("conversion_rate_ranking"),
        }
        snap = {k: v for k, v in snap.items() if v is not None}
        key = hashlib.md5(f"{name}|{snap.get('periodStart')}|{snap.get('periodEnd')}".encode("utf-8")).hexdigest()
        snap["source"] = "meta-api"
        snap["updatedAt"] = firestore.SERVER_TIMESTAMP
        ref = db.collection("adSnapshots").document(key)
        if not ref.get().exists:
            snap["createdAt"] = firestore.SERVER_TIMESTAMP
        ref.set(snap, merge=True)
        synced += 1
    return _json({"synced": synced, "datePreset": preset,
                  "adsInAccount": len(status_map) or None})


def ads_campaigns_list():
    """רשימת כל הקמפיינים בחשבון המודעות (שם, סטטוס, יעד, תקציב, תאריך יצירה) — קריאה בלבד."""
    acct = (_get_config("adAccountId") or "").strip()
    token = (_get_config("adsToken") or _get_config("fbPageToken") or "").strip()
    if not acct:
        return _json({"error": "לא הוגדר מזהה חשבון מודעות"}, 400)
    if not token:
        return _json({"error": "לא הוגדר טוקן מודעות (ads_read)"}, 400)
    acct = acct if acct.startswith("act_") else "act_" + acct
    try:
        r = requests.get(
            f"https://graph.facebook.com/v21.0/{acct}/campaigns",
            params={"fields": "name,status,effective_status,objective,daily_budget,lifetime_budget,"
                              "created_time,updated_time,start_time,stop_time",
                    "limit": 100, "access_token": token}, timeout=30)
        j = r.json()
        if "error" in j:
            return _json({"error": j["error"].get("message", "שגיאת Meta"), "raw": j["error"]}, 502)
        return _json({"campaigns": j.get("data", [])})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def ads_campaigns_performance():
    """ביצועים מצטברים לכל קמפיין מאז תאריך היצירה שלו (לא רק חלון קבוע) — כדי לזהות בזבוז/בעיית מעקב."""
    token = (_get_config("adsToken") or _get_config("fbPageToken") or "").strip()
    if not token:
        return _json({"error": "לא הוגדר טוקן מודעות (ads_read)"}, 400)
    listed = ads_campaigns_list()
    body = listed[0] if isinstance(listed, tuple) else listed
    if isinstance(body, dict) and body.get("error"):
        return listed
    campaigns = body.get("campaigns", [])
    out = []
    from datetime import date
    today = date.today().isoformat()
    for c in campaigns:
        since = (c.get("created_time") or "")[:10] or "2026-01-01"
        try:
            r = requests.get(
                f"https://graph.facebook.com/v21.0/{c['id']}/insights",
                params={"fields": "spend,impressions,inline_link_clicks,actions,action_values,purchase_roas",
                        "time_range": _jsonlib.dumps({"since": since, "until": today}),
                        "access_token": token}, timeout=30)
            j = r.json()
            row = (j.get("data") or [{}])[0] if not j.get("error") else {}
        except Exception:
            row = {}
        acts, vals = row.get("actions"), row.get("action_values")
        roas = None
        arr = row.get("purchase_roas")
        if arr:
            try:
                roas = float(arr[0].get("value"))
            except Exception:
                pass
        out.append({
            "id": c["id"], "name": c.get("name"), "status": c.get("effective_status"),
            "objective": c.get("objective"), "since": since, "until": today,
            "spend": row.get("spend"), "impressions": row.get("impressions"),
            "linkClicks": row.get("inline_link_clicks"),
            "purchases": _act_val(acts, "omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase"),
            "purchaseValue": _act_val(vals, "omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase"),
            "roas": roas,
        })
    return _json({"campaigns": out})


def ads_audiences_list():
    """קהלים מותאמים אישית (Custom Audiences) — לבדוק אם קהל 'מבקרי אתר' מבוסס-Pixel מאוכלס."""
    acct = (_get_config("adAccountId") or "").strip()
    token = (_get_config("adsToken") or _get_config("fbPageToken") or "").strip()
    if not acct:
        return _json({"error": "לא הוגדר מזהה חשבון מודעות"}, 400)
    if not token:
        return _json({"error": "לא הוגדר טוקן מודעות (ads_read)"}, 400)
    acct = acct if acct.startswith("act_") else "act_" + acct
    try:
        r = requests.get(
            f"https://graph.facebook.com/v21.0/{acct}/customaudiences",
            params={"fields": "name,subtype,approximate_count_lower_bound,approximate_count_upper_bound,"
                              "data_source,time_created,delivery_status",
                    "limit": 50, "access_token": token}, timeout=30)
        j = r.json()
        if "error" in j:
            return _json({"error": j["error"].get("message", "שגיאת Meta"), "raw": j["error"]}, 502)
        return _json({"audiences": j.get("data", [])})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def ads_pixel_stats():
    """אימות פיקסל: אילו פיקסלים בחשבון, מתי ירו לאחרונה, ואילו אירועים נורו בשבוע האחרון (לפי סוג)."""
    import time as _time
    acct = (_get_config("adAccountId") or "").strip()
    token = (_get_config("adsToken") or _get_config("fbPageToken") or "").strip()
    if not acct:
        return _json({"error": "לא הוגדר מזהה חשבון מודעות"}, 400)
    if not token:
        return _json({"error": "לא הוגדר טוקן מודעות (ads_read)"}, 400)
    acct = acct if acct.startswith("act_") else "act_" + acct
    try:
        r = requests.get(f"https://graph.facebook.com/v21.0/{acct}/adspixels",
                         params={"fields": "id,name,last_fired_time,creation_time",
                                 "limit": 25, "access_token": token}, timeout=30)
        j = r.json()
        if "error" in j:
            return _json({"error": j["error"].get("message", "שגיאת Meta"), "raw": j["error"]}, 502)
        since = int(_time.time()) - 7 * 86400
        pixels = []
        for p in j.get("data", []):
            events, stats_error = {}, None
            try:
                rs = requests.get(f"https://graph.facebook.com/v21.0/{p['id']}/stats",
                                  params={"aggregation": "event", "start_time": since,
                                          "access_token": token}, timeout=30)
                sj = rs.json()
                if "error" in sj:
                    stats_error = sj["error"].get("message")
                for bucket in (sj.get("data") or []):
                    for row in (bucket.get("data") or []):
                        ev = row.get("value") or "?"
                        events[ev] = events.get(ev, 0) + int(row.get("count") or 0)
            except Exception as e:
                stats_error = str(e)
            pixels.append({"id": p.get("id"), "name": p.get("name"),
                           "lastFired": p.get("last_fired_time"),
                           "created": p.get("creation_time"),
                           "eventsLast7d": events, "statsError": stats_error})
        return _json({"pixels": pixels})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def list_ad_snapshots():
    docs = db.collection("adSnapshots").order_by(
        "createdAt", direction=firestore.Query.DESCENDING).limit(500).stream()
    return _json({"snapshots": [_ts_to_iso(_doc_to_dict(d)) for d in docs]})


def create_ad_snapshots(data):
    """יצירת snapshot בודד או מרובה ({items:[...]} מייבוא CSV). מחזיר את הרשומות שנוצרו."""
    items = data.get("items") if isinstance(data.get("items"), list) else [data]
    if len(items) > 300:
        return _json({"error": "מקסימום 300 שורות בייבוא אחד"}, 400)
    created = []
    for item in items:
        fields = _pick(item, AD_SNAPSHOT_FIELDS)
        if not fields.get("adName"):
            continue
        fields["createdAt"] = firestore.SERVER_TIMESTAMP
        fields["updatedAt"] = firestore.SERVER_TIMESTAMP
        ref = db.collection("adSnapshots").document()
        ref.set(fields)
        created.append(ref.id)
    if not created:
        return _json({"error": "אין שורות תקינות (נדרש לפחות שם מודעה)"}, 400)
    return _json({"created": len(created), "ids": created}, 201)


def update_ad_snapshot(sid, data):
    fields = _pick(data, AD_SNAPSHOT_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("adSnapshots").document(sid)
    if not ref.get().exists:
        return _json({"error": "רשומה לא נמצאה"}, 404)
    ref.update(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())))


def delete_ad_snapshot(sid):
    ref = db.collection("adSnapshots").document(sid)
    if not ref.get().exists:
        return _json({"error": "רשומה לא נמצאה"}, 404)
    ref.delete()
    return _json({"deleted": sid})


# ── AD LAB Phase B: חוות דעת Gemini עצמאית + מסווג המלצות Meta + סיכום החלטה ──
# עקרון: Gemini מקבל רק את נתוני המודעה הגולמיים — לא רואה את ממצאי מנוע החוקים לפני שסיים,
# כדי לשמור על חוות דעת "עיוורת" ובלתי-תלויה (בדיוק כמו שתי דעות רפואיות עצמאיות).

AD_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "confidence": {"type": "number"},
        "executiveSummary": {"type": "string"},
        "whatWorks": {"type": "string"},
        "whatDoesNotWork": {"type": "string"},
        "bottleneck": {"type": "string"},
        "immediateActions": {"type": "array", "items": {"type": "string"}},
        "nextActions": {"type": "array", "items": {"type": "string"}},
        "abTests": {"type": "array", "items": {"type": "string"}},
        "missingData": {"type": "array", "items": {"type": "string"}},
        "metaRecClassification": {"type": "string"},
        "metaRecReasoning": {"type": "string"},
    },
}

AD_REVIEW_SYS = """אתה מומחה בכיר לפרסום ממומן ב-Meta (פייסבוק/אינסטגרם) עבור אתר מסחר אלקטרוני B2B — דנא ציוד אינסטלציה, ציוד מקצועי לענף האינסטלציה: איתור נזילות, צילום תרמי, מצלמות ביוב, כלי עבודה מקצועיים.

נתח את המודעה למטרת מכירות. אל תניח שכל המלצה אוטומטית של Meta נכונה. הבחן בין בעיית קריאייטיב, בעיית קהל, בעיית אתר, בעיית משפך ובעיית מעקב.

נתח לפי הסדר הבא: איכות וכמות הנתונים ← CTR קישור (לא CTR כללי) ← CPC קישור ← CPM ← תדירות ← קליקים על קישור ← צפיות דף נחיתה ← View Content ← הוספות לסל ← התחלות תשלום ← רכישות ← עלות לרכישה ← ROAS ← איכות המודעה ← ביצועים לפי פלטפורמה/מיקום ← התאמת הקריאייטיב למיקום ← התאמה בין המודעה לדף הנחיתה ← תקינות המעקב.

כללים מחייבים:
- אל תמליץ לשנות קריאייטיב רק כי אין רכישות, אם ה-CTR קישור גבוה — קודם יש לבדוק את המשפך אחרי הקליק.
- אל תמליץ לסגור מיקום/פלייסמנט אם אין בו מספיק נתונים.
- אל תציג עלות לתוצאה של 0 ₪ כשאין תוצאות בכלל — כתוב שאין נתונים מספיקים למסקנה.
- confidence (0 עד 1): כמה אתה בטוח במסקנות, לפי כמות ואיכות הנתונים שסופקו.
- כתוב בעברית, תמציתי וממוקד-פעולה.

אם סופקה "המלצת Meta להערכה" — סווג אותה לאחת מהקטגוריות הבאות בדיוק (מילה במילה): "מומלץ ליישם", "מומלץ לבדוק לפני יישום", "המלצה כללית", "לא מספיק נתונים", "לא מומלץ כרגע", "עלולה להזיק לביצועים". נמק בקצרה, בהתבסס על הנתונים בפועל של המודעה (למשל: אם Meta ממליצה לרענן קריאייטיב אך CTR קישור גבוה — הסבר שהבעיה כנראה לא בקריאייטיב אלא במשפך שאחרי הקליק). אם לא סופקה המלצת Meta — השאר metaRecClassification ו-metaRecReasoning כמחרוזת ריקה.

חשוב: את/ה מנתח/ת את המודעה באופן עצמאי לגמרי — אין לך גישה לניתוחים אחרים של המערכת, ואינך יודע/ת מה מסקנותיהם."""


def _ad_review_gemini(snap, meta_rec):
    ctx = "נתוני המודעה (JSON גולמי, ישירות מ-Meta או מהזנה ידנית):\n" + _jsonlib.dumps(snap, ensure_ascii=False, default=str)
    prompt = AD_REVIEW_SYS + "\n\n" + ctx
    if meta_rec:
        prompt += "\n\nהמלצת Meta להערכה:\n" + meta_rec
    return _call_gemini_structured(prompt, AD_REVIEW_SCHEMA)


AD_DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        "agreements": {"type": "array", "items": {"type": "string"}},
        "disagreements": {"type": "array", "items": {"type": "string"}},
        "uniqueFromRules": {"type": "array", "items": {"type": "string"}},
        "uniqueFromGemini": {"type": "array", "items": {"type": "string"}},
        "confidenceLevel": {"type": "string"},
        "finalDecision": {"type": "string"},
        "actionOrder": {"type": "array", "items": {"type": "string"}},
        "suggestedABTest": {"type": "string"},
    },
}

AD_DECISION_SYS = """אתה עורך-על שמקבל שתי חוות דעת עצמאיות על אותה מודעת Meta ממומנת — אחת ממנוע חוקים עסקי קבוע (Rules Engine), ואחת ממודל Gemini שניתח את הנתונים בעצמו — ומייצר מהן סיכום החלטה אחד ברור.

זהה בדיוק: נקודות הסכמה בין השניים; נקודות מחלוקת (ונמק בקצרה איזו עמדה נראית אמינה יותר ולמה); המלצות ייחודיות שרק אחד מהמקורות ציין; רמת ביטחון כוללת ("נמוכה"/"בינונית"/"גבוהה"); החלטה סופית מוצעת במשפט אחד ברור וישיר; סדר פעולות מומלץ (מהחשוב ביותר קודם); והצעה לניסוי A/B אחד קונקרטי אם רלוונטי (משתנה יחיד בלבד — למשל תמונה, כותרת, מחיר, CTA).

היה תמציתי, מעשי וממוקד-פעולה. כתוב בעברית."""


def ad_ai_review(data):
    data = data or {}
    snap_id = (data.get("snapshotId") or "").strip()
    meta_rec = (data.get("metaRecommendation") or "").strip()
    rules_findings = data.get("rulesFindings") or []
    if not snap_id:
        return _json({"error": "חסר snapshotId"}, 400)
    doc = db.collection("adSnapshots").document(snap_id).get()
    if not doc.exists:
        return _json({"error": "מודעה לא נמצאה"}, 404)
    snap = _doc_to_dict(doc)
    snap.pop("createdAt", None)
    snap.pop("updatedAt", None)

    gem, err = _ad_review_gemini(snap, meta_rec)
    if err or not gem:
        return _json({"error": err or "שגיאת Gemini"}, 502)

    rules_text = "\n".join(f"- [{f.get('sev')}] {f.get('title')}: {f.get('why')}" for f in rules_findings) \
        or "(מנוע החוקים לא מצא ממצאים חריגים)"
    decision_prompt = (AD_DECISION_SYS
                       + "\n\nחוות דעת א' — מנוע חוקים:\n" + rules_text
                       + "\n\nחוות דעת ב' — Gemini:\n" + _jsonlib.dumps(gem, ensure_ascii=False))
    decision, _derr = _call_gemini_structured(decision_prompt, AD_DECISION_SCHEMA)  # אם נכשל — עדיין נחזיר את חוות דעת Gemini

    db.collection("adAnalyses").document().set({
        "snapshotId": snap_id, "metaRecommendation": meta_rec or None,
        "rulesFindings": rules_findings, "geminiOpinion": gem, "decision": decision,
        "createdAt": firestore.SERVER_TIMESTAMP,
    })
    return _json({"geminiOpinion": gem, "decision": decision})


# ── AD LAB Phase C: צ'ק-ליסטים לקריאייטיב, קופי ודף נחיתה ──

def _fetch_page_text(url, limit=6000):
    """שולף טקסט גלוי מדף נחיתה (לניתוח AI) — לא סורק שדות ספציפיים, משאיר את ה-AI לקרוא ולהעריך."""
    try:
        r = requests.get(url, headers=SCAN_HEADERS, timeout=20)
        if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
            r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = re.sub(r"\n{3,}", "\n\n", soup.get_text("\n", strip=True))
        return text[:limit], None
    except Exception as e:
        return None, str(e)


AD_CREATIVE_SCHEMA = {
    "type": "object",
    "properties": {
        "creative": {
            "type": "object",
            "properties": {
                "clearIn2Seconds": {"type": "boolean"}, "productClear": {"type": "boolean"},
                "headlineClear": {"type": "boolean"}, "textOverload": {"type": "boolean"},
                "hasCTA": {"type": "boolean"}, "priceShown": {"type": "boolean"},
                "trustProof": {"type": "boolean"}, "brandingClear": {"type": "boolean"},
                "feedFit": {"type": "boolean"}, "professionalLook": {"type": "boolean"},
                "realProduct": {"type": "boolean"}, "solvesCustomerProblem": {"type": "boolean"},
                "fitsProfessionalAudience": {"type": "boolean"},
                "notes": {"type": "string"}, "suggestedAngles": {"type": "array", "items": {"type": "string"}},
            },
        },
        "copy": {
            "type": "object",
            "properties": {
                "hasOpeningHook": {"type": "boolean"}, "statesProblem": {"type": "boolean"},
                "statesSolution": {"type": "boolean"}, "listsBenefits": {"type": "boolean"},
                "hasTechnicalSpecs": {"type": "boolean"}, "hasTrustSignals": {"type": "boolean"},
                "hasWarranty": {"type": "boolean"}, "mentionsStock": {"type": "boolean"},
                "mentionsPrice": {"type": "boolean"}, "hasUrgency": {"type": "boolean"},
                "hasCTA": {"type": "boolean"}, "fitsB2B": {"type": "boolean"}, "fitsB2C": {"type": "boolean"},
                "notes": {"type": "string"},
                "shortVersion": {"type": "string"}, "mediumVersion": {"type": "string"}, "longVersion": {"type": "string"},
            },
        },
        "landingPage": {
            "type": "object",
            "properties": {
                "checked": {"type": "boolean"}, "mobileFriendlyGuess": {"type": "boolean"},
                "headlineMatchesAd": {"type": "boolean"}, "hasProductImage": {"type": "boolean"},
                "priceShown": {"type": "boolean"}, "priceIncludesVat": {"type": "boolean"},
                "stockAvailability": {"type": "boolean"}, "deliveryTimeShown": {"type": "boolean"},
                "warrantyShown": {"type": "boolean"}, "importerInfoShown": {"type": "boolean"},
                "keyBenefitsShown": {"type": "boolean"}, "techSpecsShown": {"type": "boolean"},
                "hasReviews": {"type": "boolean"}, "hasFAQ": {"type": "boolean"},
                "addToCartButton": {"type": "boolean"}, "buyButton": {"type": "boolean"},
                "paymentMethodsShown": {"type": "boolean"}, "shippingPolicyShown": {"type": "boolean"},
                "returnPolicyShown": {"type": "boolean"}, "paymentSecurityShown": {"type": "boolean"},
                "contactOptionShown": {"type": "boolean"}, "whatsappOrPhoneShown": {"type": "boolean"},
                "issues": {"type": "array", "items": {"type": "string"}}, "summary": {"type": "string"},
            },
        },
        "overallSummary": {"type": "string"},
    },
}

AD_CREATIVE_SYS = """אתה מומחה קריאייטיב ו-CRO (אופטימיזציית המרות) לפרסום ממומן ב-Meta, עבור אתר B2B לציוד אינסטלציה מקצועי בישראל (איתור נזילות, צילום תרמי, מצלמות ביוב, כלי עבודה).

תפקידך: להעריך בכנות שלושה דברים נפרדים — קריאייטיב, קופי (טקסט המודעה), ודף הנחיתה — לפי צ'ק-ליסט מדויק, ולהציע שיפורים קונקרטיים.

כללים:
- כל שדה בוליאני (true/false) חייב להתבסס רק על מה שבאמת רואים/קוראים בנתונים שסופקו. אם לא ברור או שאין מספיק מידע להעריך פריט מסוים — סמן false ואל תמציא.
- אם לא סופקה תמונת קריאייטיב כלל — השאר את כל שדות creative כ-false, ורק ב-notes כתוב "לא סופקה תמונת קריאייטיב לניתוח".
- אם לא סופק טקסט קופי כלל — השאר את שדות copy כ-false, וב-notes ציין זאת. עדיין נסה להציע shortVersion/mediumVersion/longVersion מבוססי המוצר אם יש מספיק הקשר (שם המודעה/מוצר).
- אם לא סופק טקסט מדף נחיתה — landingPage.checked=false, השאר שאר שדות landingPage כ-false, summary יסביר שלא בוצעה בדיקה.
- suggestedAngles: הצע 3-4 זוויות קריאייטיב חדשות הכי רלוונטיות למוצר הזה, מתוך: בעיה-ופתרון, מחיר, מוצר, השוואה, הדגמה, וידאו קצר, לקוח מקצועי, יתרון טכני, לפני-ואחרי, מבצע, אמינות-ואחריות. כל הצעה: משפט אחד עם שם הזווית + למה היא מתאימה כאן.
- copy.shortVersion/mediumVersion/longVersion: שלוש גרסאות קופי חדשות ומוכנות לשימוש (לא הערכה של הקיים) — קצרה (~15 מילים), בינונית (~40 מילים), ארוכה (~80 מילים) — מותאמות לקהל מקצועי B2B, כוללות CTA.
- landingPage.issues: רשימת בעיות קונקרטיות שזיהית בדף (ריקה אם אין).
- כתוב הכול בעברית, ענייני וישיר."""


def ad_creative_review(data):
    data = data or {}
    snap_id = (data.get("snapshotId") or "").strip()
    if not snap_id:
        return _json({"error": "חסר snapshotId"}, 400)
    doc = db.collection("adSnapshots").document(snap_id).get()
    if not doc.exists:
        return _json({"error": "מודעה לא נמצאה"}, 404)
    snap = _doc_to_dict(doc)

    primary_text = (data.get("primaryText") or snap.get("primaryText") or "").strip()
    headline = (data.get("headline") or snap.get("headline") or "").strip()
    description = (data.get("description") or snap.get("description") or "").strip()
    cta = (data.get("cta") or snap.get("cta") or "").strip()
    image_b64 = (data.get("creativeImageBase64") or "").strip()
    image_mime = (data.get("creativeImageMime") or "image/jpeg").strip()
    if "," in image_b64:  # data URL מלא — לוקחים רק את חלק ה-base64
        image_b64 = image_b64.split(",", 1)[1]

    landing_url = (snap.get("landingUrl") or "").strip()
    page_text, page_err = (None, None)
    if landing_url:
        page_text, page_err = _fetch_page_text(landing_url)

    ctx = [f"שם המודעה: {snap.get('adName', '')}", f"מוצר: {snap.get('product', '')}",
           f"קמפיין: {snap.get('campaign', '')}"]
    ctx.append("טקסט ראשי של המודעה: " + (primary_text or "(לא סופק)"))
    ctx.append("כותרת: " + (headline or "(לא סופקה)"))
    ctx.append("תיאור: " + (description or "(לא סופק)"))
    ctx.append("קריאה לפעולה (CTA): " + (cta or "(לא סופק)"))
    if landing_url:
        ctx.append("כתובת דף הנחיתה: " + landing_url)
        ctx.append("טקסט דף הנחיתה (חולץ אוטומטית):\n" + (page_text or f"(שליפה נכשלה: {page_err})"))
    else:
        ctx.append("לא סופקה כתובת דף נחיתה למודעה זו.")
    ctx.append("תמונת קריאייטיב: " + ("סופקה — ראה תמונה מצורפת" if image_b64 else "לא סופקה"))

    prompt = AD_CREATIVE_SYS + "\n\n" + "\n".join(ctx)
    result, err = _call_gemini_structured(prompt, AD_CREATIVE_SCHEMA,
                                          image_b64=(image_b64 or None), image_mime=image_mime)
    if err or not result:
        return _json({"error": err or "שגיאת Gemini"}, 502)

    db.collection("adCreativeReviews").document().set({
        "snapshotId": snap_id, "hasImage": bool(image_b64), "hasLandingPage": bool(landing_url),
        "result": result, "createdAt": firestore.SERVER_TIMESTAMP,
    })
    return _json(result)


def list_posts():
    """כל הפוסטים שפורסמו דרך האפליקציה + המדדים השמורים שלהם (לדוחות/למידה)."""
    docs = db.collection("posts").order_by("createdAt", direction=firestore.Query.DESCENDING).stream()
    return _json({"posts": [_ts_to_iso(_doc_to_dict(d)) for d in docs]})


def fb_scheduled_posts():
    """רשימת הפוסטים המתוזמנים בדף — ישירות מפייסבוק (כולל תזמונים ידניים מ-Business Suite)."""
    token = _get_config("fbPageToken")
    page = _get_config("fbPageId")
    if not token or not page:
        return _json({"error": "לא הוגדרו Page Token + Page ID של פייסבוק"}, 400)
    token = _resolve_page_token(token, page)
    try:
        r = requests.get(f"https://graph.facebook.com/v21.0/{page}/scheduled_posts",
                         params={"fields": "id,message,scheduled_publish_time,created_time,attachments{media_type}",
                                 "access_token": token, "limit": 50}, timeout=30)
        d = r.json()
        if "error" in d:
            return _json({"error": d["error"].get("message", "שגיאת פייסבוק"), "raw": d["error"]}, 502)
        return _json({"scheduled": d.get("data", [])})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def refresh_posts(data):
    """מושך מהפייסבוק מדדי מעורבות (לייקים/תגובות/שיתופים/ריאקציות) לכל פוסט שמור."""
    token = _resolve_page_token(_get_config("fbPageToken"), _get_config("fbPageId"))
    if not token:
        return _json({"error": "לא הוגדר חיבור פייסבוק"}, 400)
    updated = 0
    for d in db.collection("posts").stream():
        pid = d.id
        try:
            r = requests.get(
                f"https://graph.facebook.com/v21.0/{pid}",
                params={"fields": "likes.summary(true),comments.summary(true),shares,reactions.summary(true)",
                        "access_token": token}, timeout=20)
            j = r.json()
            if "error" in j:
                continue
            db.collection("posts").document(pid).update({
                "likes": ((j.get("likes") or {}).get("summary") or {}).get("total_count", 0),
                "comments": ((j.get("comments") or {}).get("summary") or {}).get("total_count", 0),
                "shares": (j.get("shares") or {}).get("count", 0),
                "reactions": ((j.get("reactions") or {}).get("summary") or {}).get("total_count", 0),
                "metricsAt": firestore.SERVER_TIMESTAMP,
            })
            updated += 1
        except Exception:
            continue
    docs = db.collection("posts").order_by("createdAt", direction=firestore.Query.DESCENDING).stream()
    return _json({"refreshed": updated, "posts": [_ts_to_iso(_doc_to_dict(x)) for x in docs]})


def ad_account_insights(data):
    """ביצועי קמפיינים ממומנים (הוצאה, חשיפות, קליקים, CTR) דרך Marketing API. דורש ads_read."""
    data = data or {}
    acct = (data.get("adAccountId") or _get_config("adAccountId") or "").strip()
    token = (_get_config("adsToken") or _get_config("fbPageToken") or "").strip()
    if not acct:
        return _json({"error": "לא הוגדר מזהה חשבון מודעות (Ad Account ID)"}, 400)
    if not token:
        return _json({"error": "לא הוגדר טוקן"}, 400)
    acct = acct if acct.startswith("act_") else "act_" + acct
    try:
        r = requests.get(
            f"https://graph.facebook.com/v21.0/{acct}/insights",
            params={"fields": "spend,impressions,clicks,ctr,cpc,reach,actions,cost_per_action_type",
                    "date_preset": data.get("datePreset") or "last_30d",
                    "level": "account", "access_token": token}, timeout=30)
        j = r.json()
        if "error" in j:
            return _json({"error": j["error"].get("message", "שגיאת Meta"), "raw": j["error"]}, 502)
        return _json({"insights": j.get("data", [])})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def scrape_product(data):
    """סורק דף מוצר מאתר המשתמש ומחזיר שם, מחיר, תיאור ותמונה — לשימוש בבניית מודעה."""
    data = data or {}
    url = (data.get("url") or "").strip()
    if not url:
        return _json({"error": "אין כתובת לסריקה"}, 400)
    try:
        r = requests.get(url, headers=SCAN_HEADERS, timeout=20)
        if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
            r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")

        def meta(*keys):
            for k in keys:
                t = soup.find("meta", property=k) or soup.find("meta", attrs={"name": k})
                if t and t.get("content"):
                    return t["content"].strip()
            return ""

        name = meta("og:title")
        if not name:
            h1 = soup.find("h1")
            if h1:
                name = h1.get_text(strip=True)
            elif soup.title and soup.title.string:
                name = soup.title.string.strip()
        desc = meta("og:description", "description", "twitter:description")
        image = meta("og:image", "twitter:image")
        price = extract_price(r.text)
        sku = ""
        sku_el = soup.find(class_="sku")
        if sku_el:
            sku = sku_el.get_text(strip=True)
        if not sku:
            for tag in soup.find_all("script", type="application/ld+json"):
                try:
                    j = _jsonlib.loads(tag.string or "")
                except Exception:
                    continue
                cand = j[0] if isinstance(j, list) and j else j
                if isinstance(cand, dict) and cand.get("sku"):
                    sku = str(cand["sku"])
                    break
        return _json({"name": name, "price": price, "description": desc, "image": image, "sku": sku, "url": url})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def lead_intake(data, args):
    """שער קליטת לידים — מקבל Webhook (למשל הזמנה/לקוח מ-WooCommerce) ויוצר 'ליד חדש' ב-CRM."""
    key = _get_config("leadIntakeKey")
    if key and (args.get("key") or "") != key:
        return _json({"error": "unauthorized"}, 401)
    data = data or {}
    billing = data.get("billing") or {}
    first = (data.get("first_name") or billing.get("first_name") or "").strip()
    last = (data.get("last_name") or billing.get("last_name") or "").strip()
    name = (first + " " + last).strip() or (data.get("name") or "").strip() or billing.get("email") or "ליד מהאתר"
    phone = (billing.get("phone") or data.get("phone") or "").strip()
    email = (data.get("email") or billing.get("email") or "").strip()
    company = (billing.get("company") or data.get("company") or "").strip()
    note_parts = []
    if data.get("id"):
        note_parts.append("הזמנה #" + str(data.get("number") or data.get("id")))
    if data.get("total"):
        note_parts.append("סכום: " + str(data.get("total")))
    items = data.get("line_items") or []
    if items:
        note_parts.append("מוצרים: " + ", ".join((i.get("name") or "") for i in items[:5]))
    if data.get("customer_note"):
        note_parts.append("הערה: " + str(data.get("customer_note")))
    note = " · ".join([p for p in note_parts if p])
    status = (data.get("status") or "").lower()
    stage = WC_STATUS_STAGE.get(status, "ליד חדש")
    order_date = data.get("date_created") or data.get("date_created_gmt") or ""
    if status:
        note = (note + " · " if note else "") + "סטטוס: " + WC_STATUS_HE.get(status, status)
    if not (phone or email or first or data.get("name")):
        return _json({"error": "no lead data"}, 400)
    # דדופ לפי טלפון/אימייל
    existing = None
    if phone:
        for d in db.collection("customers").where("phone", "==", phone).limit(1).stream():
            existing = d
    if not existing and email:
        for d in db.collection("customers").where("email", "==", email).limit(1).stream():
            existing = d
    if existing:
        ex = existing.to_dict() or {}
        upd = {"updatedAt": firestore.SERVER_TIMESTAMP}
        if note:
            prev = ex.get("notes") or ""
            upd["notes"] = (prev + "\n" if prev else "") + note
        # עדכון שלב לפי סטטוס — אם זו ההזמנה האחרונה/עדכון לאותה הזמנה, ורק אם הסטטוס ממופה
        if status and (not order_date or order_date >= (ex.get("lastOrderAt") or "")):
            if status in WC_STATUS_STAGE:
                upd["stage"] = stage
            if order_date:
                upd["lastOrderAt"] = order_date
        db.collection("customers").document(existing.id).update(upd)
        return _json({"ok": True, "matched": existing.id})
    fields = {
        "name": name, "phone": phone, "email": email, "company": company,
        "stage": stage, "source": "אתר", "notes": note, "lastOrderAt": order_date,
        "createdAt": firestore.SERVER_TIMESTAMP, "updatedAt": firestore.SERVER_TIMESTAMP,
    }
    ref = db.collection("customers").document()
    ref.set(fields)
    return _json({"ok": True, "id": ref.id}, 201)


WC_STATUS_STAGE = {
    # סטנדרטיים
    "pending": "ליד חדש", "on-hold": "ליד חדש", "checkout-draft": "ליד חדש",
    "processing": "בטיפול",
    "completed": "סופק",
    "cancelled": "אבוד", "refunded": "אבוד", "failed": "אבוד",
    # ניחושים לסטטוסים מותאמים (משלוח/הצעות) — יעודכן לפי הסלאגים האמיתיים מ-seen
    "shipping": "משלוח", "delivered": "סופק", "ready-pickup": "סופק", "ready-for-pickup": "סופק",
    "quote-request": "ליד חדש", "new-quote": "ליד חדש", "request-quote": "ליד חדש",
    "quote-pending": "הצעה ממתינה", "pending-quote": "הצעה ממתינה", "quote-sent": "הצעה ממתינה",
    "quote-accepted": "הצעה ממתינה", "quote-approved": "הצעה ממתינה",
    "quote-rejected": "אבוד", "quote-expired": "אבוד", "expired": "אבוד",
}
WC_STATUS_HE = {
    "pending": "ממתין לתשלום", "on-hold": "בהמתנה", "processing": "בטיפול",
    "completed": "הושלם", "cancelled": "בוטל", "refunded": "זוכה",
    "failed": "נכשל", "checkout-draft": "טיוטה",
}


def wc_find(args):
    """מאתר מוצר באתר WooCommerce לפי מק"ט (sku) או שם (search) — מחזיר קישור, שם, מחיר."""
    base = (_get_config("wcApiBase") or "").rstrip("/")
    ck = _get_config("wcKey")
    cs = _get_config("wcSecret")
    if not (base and ck and cs):
        return _json({"error": "לא הוגדרו פרטי WooCommerce API (כתובת, Key, Secret)"}, 400)
    if not base.startswith("http"):
        base = "https://" + base
    sku = (args.get("sku") or "").strip()
    search = (args.get("search") or "").strip()
    params = {"consumer_key": ck, "consumer_secret": cs, "per_page": 10}
    if sku:
        params["sku"] = sku
    elif search:
        params["search"] = search
    else:
        return _json({"error": "חסר sku או search"}, 400)
    try:
        r = requests.get(base + "/wp-json/wc/v3/products", params=params, timeout=45)
        items = r.json()
        if isinstance(items, dict):
            return _json({"error": items.get("message", "שגיאת WooCommerce")}, 502)
        out = [{"id": p.get("id"), "name": p.get("name"), "sku": p.get("sku"),
                "price": p.get("price"), "permalink": p.get("permalink"),
                "status": p.get("status")} for p in items]
        return _json({"products": out})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def wc_sync(data):
    """מושך את ההזמנות האחרונות מ-WooCommerce, ממפה סטטוס→שלב, ומייבא/מעדכן לידים ב-CRM."""
    data = data or {}
    base = (_get_config("wcApiBase") or "").rstrip("/")
    ck = _get_config("wcKey")
    cs = _get_config("wcSecret")
    if not (base and ck and cs):
        return _json({"error": "לא הוגדרו פרטי WooCommerce API (כתובת, Key, Secret)"}, 400)
    if not base.startswith("http"):
        base = "https://" + base
    per_page = min(max(int(data.get("perPage") or 10), 1), 100)
    imported, updated = 0, 0
    seen = {}
    try:
        r = requests.get(base + "/wp-json/wc/v3/orders",
                         params={"consumer_key": ck, "consumer_secret": cs,
                                 "per_page": per_page, "orderby": "date", "order": "desc"}, timeout=45)
        orders = r.json()
        if isinstance(orders, dict):
            return _json({"error": orders.get("message", "שגיאת WooCommerce")}, 502)
        for o in orders:
            status = (o.get("status") or "").lower()
            seen[status] = seen.get(status, 0) + 1
            b = o.get("billing") or {}
            phone = (b.get("phone") or "").strip()
            email = (b.get("email") or "").strip()
            name = ((b.get("first_name") or "") + " " + (b.get("last_name") or "")).strip() or email or "ליד מהאתר"
            if not (phone or email):
                continue
            stage = WC_STATUS_STAGE.get(status, "ליד חדש")
            note = "הזמנה #" + str(o.get("number") or o.get("id")) + " · סטטוס: " + WC_STATUS_HE.get(status, status or "—")
            if o.get("total"):
                note += " · ₪" + str(o.get("total"))
            existing = None
            if phone:
                for d in db.collection("customers").where("phone", "==", phone).limit(1).stream():
                    existing = d
            if not existing and email:
                for d in db.collection("customers").where("email", "==", email).limit(1).stream():
                    existing = d
            order_date = o.get("date_created") or o.get("date_created_gmt") or ""
            if existing:
                ex = existing.to_dict() or {}
                prev = ex.get("notes") or ""
                upd = {"notes": (prev + "\n" if prev else "") + note, "updatedAt": firestore.SERVER_TIMESTAMP}
                # עדכון שלב אם זו ההזמנה האחרונה או עדכון לאותה הזמנה (>=) — ורק אם הסטטוס ממופה
                if order_date and order_date >= (ex.get("lastOrderAt") or ""):
                    if status in WC_STATUS_STAGE:
                        upd["stage"] = WC_STATUS_STAGE[status]
                    upd["lastOrderAt"] = order_date
                db.collection("customers").document(existing.id).update(upd)
                updated += 1
            else:
                db.collection("customers").document().set({
                    "name": name, "phone": phone, "email": email, "company": (b.get("company") or "").strip(),
                    "stage": stage, "source": "אתר", "notes": note, "lastOrderAt": order_date,
                    "createdAt": firestore.SERVER_TIMESTAMP, "updatedAt": firestore.SERVER_TIMESTAMP,
                })
                imported += 1
        # רשימת כל סטטוסי ההזמנות באתר (קוד → שם בעברית) — לאבחון מיפוי
        status_list = []
        try:
            rs = requests.get(base + "/wp-json/wc/v3/reports/orders/totals",
                              params={"consumer_key": ck, "consumer_secret": cs}, timeout=20)
            js = rs.json()
            if isinstance(js, list):
                for s in js:
                    status_list.append({"slug": (s.get("slug") or "").replace("wc-", ""),
                                        "name": s.get("name") or "", "total": s.get("total") or 0})
        except Exception:
            pass
        return _json({"ok": True, "imported": imported, "updated": updated,
                      "total": len(orders), "seen": seen, "statusList": status_list})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def shorten_url(data):
    """מקצר כתובת URL (TinyURL, עם נפילה ל-is.gd; אם נכשל — מחזיר את המקור)."""
    data = data or {}
    url = (data.get("url") or "").strip()
    if not url:
        return _json({"error": "אין כתובת לקיצור"}, 400)
    for endpoint, params in (
        ("https://tinyurl.com/api-create.php", {"url": url}),
        ("https://is.gd/create.php", {"format": "simple", "url": url}),
    ):
        try:
            r = requests.get(endpoint, params=params, timeout=12)
            short = (r.text or "").strip()
            if r.status_code == 200 and short.startswith("http"):
                return _json({"short": short})
        except Exception:
            continue
    return _json({"short": url})  # נפילה: הכתובת המקורית


def ads_library(data):
    """מושך מודעות מתחרים מספריית המודעות של Meta (Facebook/Instagram). קריאה בלבד."""
    data = data or {}
    token = _get_config("metaToken")
    if not token:
        return _json({"error": "לא הוגדר Meta Access Token"}, 400)
    terms = (data.get("searchTerms") or "").strip()
    page_ids = data.get("searchPageIds") or []
    if not terms and not page_ids:
        return _json({"error": "יש להזין מילת חיפוש או מזהה דף"}, 400)
    params = {
        "access_token": token,
        "ad_reached_countries": _jsonlib.dumps(data.get("countries") or ["IL"]),
        "ad_type": data.get("adType") or "ALL",
        "ad_active_status": data.get("activeStatus") or "ACTIVE",
        "fields": "id,page_id,page_name,ad_creative_bodies,ad_creative_link_titles,"
                  "ad_snapshot_url,ad_delivery_start_time,publisher_platforms",
        "limit": min(int(data.get("limit") or 25), 50),
    }
    if terms:
        params["search_terms"] = terms
    if page_ids:
        params["search_page_ids"] = _jsonlib.dumps(page_ids)
    try:
        r = requests.get("https://graph.facebook.com/v21.0/ads_archive", params=params, timeout=30)
        d = r.json()
        if "error" in d:
            return _json({"error": d["error"].get("message", "שגיאת Meta"), "raw": d["error"]}, 502)
        ads = []
        for a in d.get("data", []):
            ads.append({
                "id": a.get("id"),
                "pageId": a.get("page_id"),
                "pageName": a.get("page_name"),
                "body": (a.get("ad_creative_bodies") or [""])[0],
                "title": (a.get("ad_creative_link_titles") or [""])[0],
                "url": a.get("ad_snapshot_url"),
                "start": a.get("ad_delivery_start_time"),
                "platforms": a.get("publisher_platforms") or [],
            })
        return _json({"ads": ads, "count": len(ads)})
    except Exception as e:
        return _json({"error": str(e)}, 500)


PRICE_TEXT = re.compile(r'(?:₪|ש"?ח|NIS)\s*[\d.,]+|[\d.,]{2,}\s*(?:₪|ש"?ח|NIS)')


def scrape_category(url):
    """שולף מוצרים (שם · קישור · מחיר) מעמוד קטגוריה. מנסה WooCommerce, ואז היוריסטיקה
    גנרית (כרטיס מוצר = אלמנט שמכיל גם קישור וגם מחיר) שעובדת על פלטפורמות שונות."""
    r = requests.get(url, headers=SCAN_HEADERS, timeout=25)
    if r.status_code != 200:
        return {"error": f"שגיאת טעינת הקטגוריה (status {r.status_code})"}
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    products, seen = [], set()

    # 1) WooCommerce (האתר של דנא)
    for li in soup.select("li.product"):
        a = li.select_one("a.woocommerce-loop-product__link") or li.find("a", href=True)
        if not a or not a.get("href") or "/product/" not in a["href"]:
            continue
        href = a["href"].split("?")[0]
        if href in seen:
            continue
        t = li.select_one(".woocommerce-loop-product__title")
        name = t.get_text(strip=True) if t else a.get_text(strip=True)
        pe = li.select_one(".woocommerce-Price-amount bdi") or li.select_one(".woocommerce-Price-amount")
        price = _to_price(pe.get_text()) if pe else None
        seen.add(href)
        products.append({"name": name, "myUrl": href, "myPrice": price})
    if products:
        return {"products": products}

    base = url
    # 2) משפחת WooCommerce לפי קישורי /product/ (ערכות כמו WoodMart — smartliner)
    plinks = {}
    for a in soup.select('a[href*="/product/"]'):
        href = urljoin(base, a["href"].split("?")[0])
        if "/product-category/" in href or href in plinks:
            continue
        name = (a.get("aria-label") or a.get("title") or a.get_text(" ", strip=True) or "").strip()
        if not name:
            img = a.find("img")
            name = (img.get("alt") or "").strip() if img else ""
        price, node = None, a
        for _ in range(6):
            if node is None:
                break
            pe = node.select_one(".woocommerce-Price-amount bdi, .woocommerce-Price-amount, .price bdi")
            if pe:
                price = _to_price(pe.get_text())
                break
            node = node.parent
        if name and len(name) >= 3:
            plinks[href] = {"name": name, "myUrl": href, "myPrice": price}
    if plinks:
        return {"products": list(plinks.values())}

    # 3) היוריסטיקה גנרית: לכל טקסט מחיר, מטפסים למעלה עד מציאת קישור מוצר
    for node in soup.find_all(string=PRICE_TEXT):
        price = _to_price(PRICE_TEXT.search(node).group(0))
        if price is None:
            continue
        el, a = node.parent, None
        for _ in range(6):
            if el is None:
                break
            a = el.find("a", href=True)
            if a:
                break
            el = el.parent
        if not a or not a.get("href"):
            continue
        href = urljoin(base, a["href"].split("?")[0])
        if href in seen or href.rstrip('/') == base.rstrip('/'):
            continue
        name = a.get_text(" ", strip=True) or a.get("title") or ""
        if not name:
            img = a.find("img")
            name = (img.get("alt") or "") if img else ""
        name = name.strip()
        if not name or len(name) < 3:
            continue
        seen.add(href)
        products.append({"name": name, "myUrl": href, "myPrice": price})
        if len(products) >= 80:
            break
    return {"products": products}


OUR_DOMAIN = "dna-tools.co.il"


def extract_model_id(name):
    """מחלץ מזהה דגם (מותג+דגם, בד\"כ בלטינית) משם המוצר, ואת הטוקן המבחין."""
    toks = re.findall(r'[A-Za-z0-9][A-Za-z0-9.\-]*', name or '')
    toks = [t for t in toks if len(t) >= 2 or any(c.isdigit() for c in t)]
    search = ' '.join(toks[:6]).strip()
    token = ''
    # 1. קוד דגם אמיתי = שילוב אותיות+ספרות (E96, C5, ETS320, i64) — לא מספר מפרט כמו 0.03
    for t in toks:
        if re.search(r'[A-Za-z]', t) and re.search(r'\d', t):
            token = t
            break
    # 2. אחרת — הטוקן הראשון שמכיל ספרה
    if not token:
        for t in toks:
            if any(c.isdigit() for c in t):
                token = t
                break
    # 3. אחרת — הטוקן האחרון
    if not token and toks:
        token = toks[-1]
    return (search or (name or '').strip()), token


def _domain(url):
    m = re.search(r'https?://([^/]+)', url or '')
    return m.group(1).lower().replace('www.', '') if m else ''


def _norm_domains(lst):
    """מנרמל רשימת קישורים/דומיינים לדומיינים נקיים (ksp.co.il)."""
    out = []
    for x in (lst or []):
        d = re.sub(r'^https?://', '', (x or '').strip().lower())
        d = d.replace('www.', '').split('/')[0]
        if d:
            out.append(d)
    return out


def validate_candidate(url, token):
    """נכנס לדף המתחרה ובודק: האם מזהה הדגם מופיע, והאם יש מחיר."""
    try:
        r = requests.get(url, headers=SCAN_HEADERS, timeout=15)
        if r.status_code != 200:
            return {"price": None, "hasToken": False, "status": r.status_code}
        if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
            r.encoding = r.apparent_encoding or "utf-8"
        html = r.text
        return {"price": extract_price(html),
                "hasToken": bool(token) and token.lower() in html.lower(),
                "status": 200}
    except Exception:
        return {"price": None, "hasToken": False, "status": "error"}


def serp_competitors(query, token, max_validate=8, only_domains=None):
    """SERP (אורגני + ממומן) → מועמדים, ואימות כל אחד מול הדף בפועל (במקביל).
    אם only_domains ניתן — החיפוש ממוקד רק למתחרים האלה; אחרת כל אתר .il."""
    key = _get_config("serpApiKey")
    if not key:
        return []
    only_domains = only_domains or []
    # כשמוגדרים מתחרים — חיפוש ממוקד-אתר (site:) ששואל את גוגל ישירות אם הדגם קיים אצלם,
    # גם אם הם לא מדורגים גבוה. אחרת — חיפוש כללי בכל אתרי .il.
    q = f'"{query}"'
    if only_domains:
        q += " (" + " OR ".join(f"site:{d}" for d in only_domains) + ")"
    cap = max(max_validate, len(only_domains) + 2) if only_domains else max_validate
    try:
        params = {"engine": "google", "q": q,
                  "gl": "il", "hl": "he", "num": 20, "api_key": key}
        r = requests.get("https://serpapi.com/search.json", params=params, timeout=30)
        data = r.json()
    except Exception:
        return []

    seen, cands = set(), []
    for it in (data.get("ads") or []) + (data.get("organic_results") or []):
        link = it.get("link")
        dom = _domain(link)
        if not dom or OUR_DOMAIN in dom or dom in seen:
            continue
        if only_domains:
            if not any(kd in dom for kd in only_domains):   # רק המתחרים שהוגדרו
                continue
        elif not dom.endswith(".il"):                       # אחרת — כל אתר ישראלי
            continue
        seen.add(dom)
        cands.append({"url": link, "name": it.get("source") or it.get("displayed_link") or dom})
        if len(cands) >= cap:
            break

    with ThreadPoolExecutor(max_workers=6) as ex:
        validations = list(ex.map(lambda c: validate_candidate(c["url"], token), cands))

    out = []
    for c, v in zip(cands, validations):
        verified = bool(v["hasToken"]) and v["price"] is not None
        if verified:
            reason = ""
        elif v["hasToken"]:
            reason = "הדגם נמצא אך לא זוהה מחיר"
        elif v["price"] is not None:
            reason = "נמצא מחיר אך הדגם לא אומת"
        else:
            reason = "הדגם לא נמצא בדף"
        out.append({"name": c["name"], "url": c["url"], "price": v["price"],
                    "verified": verified, "reason": reason})
    out.sort(key=lambda x: 0 if x["verified"] else 1)
    return out


def discover_category(data):
    url = (data or {}).get("categoryUrl", "").strip()
    if not url:
        return _json({"error": "שדה 'categoryUrl' הוא חובה"}, 400)
    res = scrape_category(url)
    if "error" in res:
        return _json(res, 502)
    products = res["products"]
    serp_on = bool(_get_config("serpApiKey"))
    for p in products:
        search, token = extract_model_id(p["name"])
        p["searchTerm"] = search
        p["modelToken"] = token
        p["competitors"] = []   # נטען לכל דגם בנפרד (/search-competitors) — מהיר ולא חורג ממכסת SERP
    return _json({"products": products, "count": len(products), "serpConfigured": serp_on})


def search_competitors_api(data):
    """חיפוש מתחרים יחיד לפי מונח (כשהמשתמש עורך את מונח החיפוש לדגם)."""
    q = (data or {}).get("query", "").strip()
    if not q:
        return _json({"error": "שדה 'query' הוא חובה"}, 400)
    _, token = extract_model_id(q)
    token = (data or {}).get("token") or token
    only = _norm_domains((data or {}).get("competitors"))
    return _json({"competitors": serp_competitors(q, token, only_domains=only)})


def create_tracked(data):
    """יצירת דגם מקישורים ידניים בלבד: הקישור שלי + קישורי מתחרים → סריקת מחירים.
    פוסל אם אין מחיר שלי או של אף מתחרה, ומונע כפילות לפי הקישור שלי."""
    data = data or {}
    name = (data.get("name") or "").strip()
    my_url = (data.get("myUrl") or "").strip()
    comp_urls = [u.strip() for u in (data.get("competitorUrls") or []) if u and u.strip()]
    if not my_url:
        return _json({"error": "הקישור שלך הוא חובה"}, 400)
    if not comp_urls:
        return _json({"error": "יש להזין לפחות קישור מתחרה אחד"}, 400)
    if not name:   # שם אופציונלי — נגזר מה-slug של הקישור
        seg = unquote(my_url.rstrip('/').split('?')[0].split('/')[-1])
        name = seg.replace('-', ' ').strip() or "מוצר"

    # סריקת המחיר שלי
    my = fetch_price(my_url)
    if my["price"] is None:
        return _json({"error": "לא נמצא מחיר בקישור שלך — בדוק שהקישור נכון ומכיל מחיר"}, 422)

    # סריקת מחירי המתחרים במקביל; שומרים רק כאלה עם מחיר
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(lambda u: (u, fetch_price(u)), comp_urls))
    valid = [(u, r["price"]) for u, r in results if r["price"] is not None]
    if not valid:
        return _json({"error": "לא נמצא מחיר אצל אף אחד מהמתחרים — בדוק את הקישורים"}, 422)

    try:
        cost = float(data.get("myCost")) if data.get("myCost") not in (None, "") else None
    except Exception:
        cost = None

    now = firestore.SERVER_TIMESTAMP
    # אם הדגם כבר קיים (אותו myUrl) — מוסיפים אליו את המתחרים (מיזוג), לא חוסמים
    existing = list(db.collection("models").where("myUrl", "==", my_url).limit(1).stream())
    if existing:
        mref = existing[0].reference
        upd = {"myPrice": my["price"], "lastScanAt": now, "updatedAt": now}
        if cost is not None:
            upd["myCost"] = cost
        mref.update(upd)
        have = {s.to_dict().get("url") for s in
                db.collection("priceSources").where("modelId", "==", mref.id).stream()}
    else:
        mref = db.collection("models").document()
        mref.set({"name": name, "myUrl": my_url, "myPrice": my["price"], "myCost": cost,
                  "status": "active", "lastScanAt": now, "createdAt": now, "updatedAt": now})
        have = set()

    for u, price in valid:
        if u in have:                      # מתחרה שכבר קיים — מדלגים
            continue
        db.collection("priceSources").document().set({
            "modelId": mref.id, "url": u, "name": _domain(u),
            "lastPrice": price, "lastScanAt": now, "lastStatus": 200, "createdAt": now})

    m = _ts_to_iso(_doc_to_dict(mref.get()))
    m["competitors"] = _sources_for(mref.id)
    return _json({"model": m, "skippedCompetitors": len(comp_urls) - len(valid),
                  "merged": bool(existing)}, 201)


def track_categories(data):
    """קלט: קישור קטגוריה שלי + קישורי קטגוריה של מתחרים. שולפים את כל המוצרים מכל
    הקטגוריות (כולל מחירים), מתאימים לפי מזהה דגם, ויוצרים דגם לכל מוצר שלי עם התאמה."""
    data = data or {}
    my_cat = (data.get("myCategoryUrl") or "").strip()
    comp_cats = [u.strip() for u in (data.get("competitorCategoryUrls") or []) if u and u.strip()]
    if not my_cat:
        return _json({"error": "הקישור לקטגוריה שלך הוא חובה"}, 400)
    if not comp_cats:
        return _json({"error": "יש להזין לפחות קטגוריית מתחרה אחת"}, 400)

    mine = scrape_category(my_cat)
    if "error" in mine:
        return _json({"error": "לא ניתן לטעון את הקטגוריה שלך: " + mine["error"]}, 502)
    my_products = mine["products"]

    def _scrape(u):
        res = scrape_category(u)
        return (_domain(u), res.get("products", []) if "error" not in res else [])

    with ThreadPoolExecutor(max_workers=5) as ex:
        comp_results = list(ex.map(_scrape, comp_cats))

    # אינדקס מוצרי מתחרים לפי טוקן דגם
    comp_by_token = {}
    comp_found = 0
    for dom, prods in comp_results:
        for p in prods:
            if p.get("myPrice") is None:
                continue
            _, tok = extract_model_id(p["name"])
            if not tok:
                continue
            comp_found += 1
            comp_by_token.setdefault(tok.lower(), []).append(
                {"name": dom, "url": p["myUrl"], "price": p["myPrice"]})

    now = firestore.SERVER_TIMESTAMP
    created, skipped = 0, 0
    for mp in my_products:
        if mp.get("myPrice") is None:
            skipped += 1
            continue
        _, mtok = extract_model_id(mp["name"])
        matches = comp_by_token.get(mtok.lower(), []) if mtok else []
        if not matches:
            skipped += 1
            continue
        if list(db.collection("models").where("myUrl", "==", mp["myUrl"]).limit(1).stream()):
            skipped += 1
            continue
        mref = db.collection("models").document()
        mref.set({"name": mp["name"], "myUrl": mp["myUrl"], "myPrice": mp["myPrice"],
                  "status": "active", "lastScanAt": now, "createdAt": now, "updatedAt": now})
        seen_dom = set()
        for c in matches:
            if c["name"] in seen_dom:        # מתחרה אחד לכל דומיין
                continue
            seen_dom.add(c["name"])
            db.collection("priceSources").document().set({
                "modelId": mref.id, "url": c["url"], "name": c["name"],
                "lastPrice": c["price"], "lastScanAt": now, "lastStatus": 200, "createdAt": now})
        created += 1

    return _json({"created": created, "skipped": skipped,
                  "myProducts": len(my_products), "competitorProducts": comp_found})


# ═══════════════════════════ CATALOG (תמחור) + FX ═══════════════════════════

CATALOG_FIELDS = {"sku", "name", "usdPrice", "ilsManual", "shippingPct", "desiredProfit", "modelId", "category"}


def get_fx():
    """שער דולר→שקל חי (USD→ILS)."""
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=15)
        rate = (r.json().get("rates") or {}).get("ILS")
        if rate:
            return _json({"rate": round(float(rate), 4)})
    except Exception:
        pass
    return _json({"rate": None})


def fetch_image_b64(url):
    """תמונת המוצר הראשית כ-data URL (base64) — מדף מוצר (og:image) או מתמונה ישירה.
    משמש את מנוע התמונות כדי לייצר ויזואל מבוסס המוצר האמיתי."""
    try:
        r = requests.get(url, headers=SCAN_HEADERS, timeout=20)
        if r.status_code != 200:
            return _json({"error": f"status {r.status_code}"}, 502)
        ctype = (r.headers.get("Content-Type") or "").split(";")[0]
        if ctype.startswith("image/"):
            if len(r.content) > 5_000_000:
                return _json({"error": "התמונה גדולה מדי"}, 413)
            return _json({"dataUrl": f"data:{ctype};base64," + base64.b64encode(r.content).decode()})
        # דף HTML → מאתרים את תמונת המוצר
        if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
            r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        img_url = None
        for attrs in ({"property": "og:image"}, {"property": "og:image:url"}, {"name": "twitter:image"}):
            m = soup.find("meta", attrs=attrs)
            if m and m.get("content"):
                img_url = m["content"]
                break
        if not img_url:
            el = soup.select_one(".woocommerce-product-gallery__image img, img.wp-post-image")
            if el:
                img_url = el.get("src") or el.get("data-src")
        if not img_url:
            return _json({"error": "לא נמצאה תמונת מוצר בדף"}, 404)
        ir = requests.get(urljoin(url, img_url), headers=SCAN_HEADERS, timeout=20)
        ictype = (ir.headers.get("Content-Type") or "").split(";")[0]
        if ir.status_code != 200 or not ictype.startswith("image/"):
            return _json({"error": "שגיאה בטעינת התמונה"}, 502)
        if len(ir.content) > 5_000_000:
            return _json({"error": "התמונה גדולה מדי"}, 413)
        return _json({"dataUrl": f"data:{ictype};base64," + base64.b64encode(ir.content).decode()})
    except Exception as e:
        return _json({"error": str(e)}, 500)


def _og_image_url(url):
    """מחזיר את כתובת תמונת המוצר (og:image) — כתובת ציבורית להזנה ל-Placid."""
    try:
        r = requests.get(url, headers=SCAN_HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        for attrs in ({"property": "og:image"}, {"property": "og:image:url"}, {"name": "twitter:image"}):
            m = soup.find("meta", attrs=attrs)
            if m and m.get("content"):
                return urljoin(url, m["content"])
        el = soup.select_one(".woocommerce-product-gallery__image img, img.wp-post-image")
        if el:
            return urljoin(url, el.get("src") or el.get("data-src") or "")
    except Exception:
        pass
    return None


def design_image(data):
    """מעצב תמונת קמפיין דרך Ideogram (חזק ברינדור טקסט בתמונה) מתוך prompt."""
    data = data or {}
    key = _get_config("ideogramKey")
    if not key:
        return _json({"error": "לא הוגדר מפתח Ideogram"}, 400)
    prompt = (data.get("prompt") or "").strip()
    if not prompt:
        return _json({"error": "חסר תיאור (prompt)"}, 400)
    body = {"image_request": {
        "prompt": prompt,
        "aspect_ratio": data.get("aspectRatio") or "ASPECT_1_1",
        "model": data.get("model") or "V_2",
        "magic_prompt_option": "AUTO",
    }}
    try:
        r = requests.post("https://api.ideogram.ai/generate",
                          headers={"Api-Key": key, "Content-Type": "application/json"},
                          json=body, timeout=90)
        d = r.json()
        arr = d.get("data") or []
        url = arr[0].get("url") if arr else None
        if url:
            return _json({"imageUrl": url})
        return _json({"error": d.get("error") or d.get("message") or "Ideogram לא החזיר תמונה", "raw": d}, 502)
    except Exception as e:
        return _json({"error": str(e)}, 500)


def list_catalog():
    docs = db.collection("catalog").order_by(
        "createdAt", direction=firestore.Query.DESCENDING).stream()
    return _json({"items": [_ts_to_iso(_doc_to_dict(d)) for d in docs]})


def create_catalog(data):
    fields = _pick(data, CATALOG_FIELDS)
    if not fields.get("name") and not fields.get("sku"):
        return _json({"error": "יש להזין שם מוצר או מק\"ט"}, 400)
    fields.setdefault("shippingPct", 10)
    fields.setdefault("desiredProfit", 0.7)
    # מניעת כפילויות: אם קיים פריט עם אותו מק"ט — מעדכנים אותו במקום ליצור חדש (upsert)
    sku = (fields.get("sku") or "").strip()
    if sku:
        existing = list(db.collection("catalog").where("sku", "==", sku).limit(1).stream())
        if existing:
            ref = existing[0].reference
            fields["updatedAt"] = firestore.SERVER_TIMESTAMP
            ref.update(fields)
            return _json(_ts_to_iso(_doc_to_dict(ref.get())), 200)
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("catalog").document()
    ref.set(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())), 201)


def update_catalog(cid, data):
    fields = _pick(data, CATALOG_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("catalog").document(cid)
    if not ref.get().exists:
        return _json({"error": "פריט לא נמצא"}, 404)
    ref.update(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())))


def delete_catalog(cid):
    ref = db.collection("catalog").document(cid)
    if not ref.get().exists:
        return _json({"error": "פריט לא נמצא"}, 404)
    ref.delete()
    return _json({"deleted": cid})


# ═══════════════════════════ ROUTER ═══════════════════════════

@functions_framework.http
def competitors_api(request):
    if request.method == "OPTIONS":
        return ("", 204, CORS)

    # אימות מפתח (אם הוגדר API_KEY)
    if API_KEY and request.headers.get("X-API-Key") != API_KEY:
        return _json({"error": "לא מורשה"}, 401)

    # פירוק הנתיב לחלקים: /competitors/{id} → ["competitors", "{id}"]
    parts = [p for p in (request.path or "").split("/") if p]
    resource = parts[0] if parts else ""
    item_id = parts[1] if len(parts) > 1 else None
    method = request.method
    data = request.get_json(silent=True) or {}
    args = request.args

    try:
        if resource == "competitors":
            if method == "GET":
                return list_competitors()
            if method == "POST":
                return create_competitor(data)
            if method == "PUT" and item_id:
                return update_competitor(item_id, data)
            if method == "DELETE" and item_id:
                return delete_competitor(item_id)

        elif resource == "pages":
            if method == "GET":
                return list_pages(args.get("competitorId"))
            if method == "POST":
                return create_page(data)
            if method == "PUT" and item_id:
                return update_page(item_id, data)
            if method == "DELETE" and item_id:
                return delete_page(item_id)

        elif resource == "models":
            if method == "GET":
                return list_models()
            if method == "POST":
                return create_model(data)
            if method == "PUT" and item_id:
                return update_model(item_id, data)
            if method == "DELETE" and item_id:
                return delete_model(item_id)

        elif resource == "sources":
            if method == "POST":
                return create_source(data)
            if method == "PUT" and item_id:
                return update_source(item_id, data)
            if method == "DELETE" and item_id:
                return delete_source(item_id)

        elif resource == "scan":
            if method == "POST" and item_id:
                return scan_model(item_id)

        elif resource == "scan-all":
            if method in ("GET", "POST"):
                # סריקה אוטומטית (Cloud Scheduler) רצה רק בשבת ובמצב חמקני — להקטין חשיפה לזיהוי/חסימה.
                # סריקה ידנית מהאתר (דפדפן) או ?force=1 — תמיד רצה, מהר.
                ua = request.headers.get("User-Agent") or ""
                is_cron = "Google-Cloud-Scheduler" in ua
                force = (args.get("force") or "").lower() in ("1", "true", "yes")
                if is_cron and not force:
                    from datetime import datetime
                    try:
                        from zoneinfo import ZoneInfo
                        now = datetime.now(ZoneInfo("Asia/Jerusalem"))
                    except Exception:
                        now = datetime.utcnow()
                    if now.weekday() != 5:  # 5 = שבת (שני=0 … שבת=5)
                        return _json({"skipped": "סריקה אוטומטית רצה רק בשבת", "weekday": now.weekday()})
                return scan_all(stealth=is_cron and not force)

        elif resource == "fx":
            if method == "GET":
                return get_fx()

        elif resource == "img":
            if method == "GET":
                u = args.get("url")
                if not u:
                    return _json({"error": "חסר url"}, 400)
                return fetch_image_b64(u)

        elif resource == "design":
            if method == "POST":
                return design_image(data)

        elif resource == "ads-library":
            if method == "POST":
                return ads_library(data)

        elif resource == "publish-facebook":
            if method == "POST":
                return publish_facebook(data)

        elif resource == "shorten":
            if method == "POST":
                return shorten_url(data)

        elif resource == "scrape-product":
            if method == "POST":
                return scrape_product(data)

        elif resource == "lead-intake":
            if method == "POST":
                return lead_intake(data, args)

        elif resource == "wc-sync":
            if method == "POST":
                return wc_sync(data)

        elif resource == "wc-find":
            if method == "GET":
                return wc_find(args)

        elif resource == "customers":
            if method == "GET":
                return list_customers()
            if method == "POST":
                return create_customer(data)
            if method == "PUT" and item_id:
                return update_customer(item_id, data)
            if method == "DELETE" and item_id:
                return delete_customer(item_id)
            if method == "DELETE" and not item_id:
                return delete_all_customers()

        elif resource == "documents":
            if method == "GET":
                return list_documents(args.get("customerId"))
            if method == "POST":
                return create_document(data)
            if method == "DELETE" and item_id:
                return delete_document(item_id)

        elif resource == "tasks":
            if method == "GET":
                return list_tasks(args.get("customerId"))
            if method == "POST":
                return create_task(data)
            if method == "PUT" and item_id:
                return update_task(item_id, data)
            if method == "DELETE" and item_id:
                return delete_task(item_id)

        elif resource == "assistant":
            if method == "GET":
                return list_assistant_messages()
            if method == "POST":
                return assistant_process(data)

        elif resource == "email-inbox":
            if method == "GET":
                return email_inbox(args)

        elif resource == "email-task":
            if method == "POST":
                return email_task(data)

        elif resource == "ad-snapshots":
            if method == "GET":
                return list_ad_snapshots()
            if method == "POST":
                return create_ad_snapshots(data)
            if method == "PUT" and item_id:
                return update_ad_snapshot(item_id, data)
            if method == "DELETE" and item_id:
                return delete_ad_snapshot(item_id)

        elif resource == "ads-sync":
            if method == "POST":
                return ads_sync(data)

        elif resource == "ads-campaigns":
            if method == "GET":
                return ads_campaigns_list()

        elif resource == "ads-campaigns-performance":
            if method == "GET":
                return ads_campaigns_performance()

        elif resource == "ads-audiences":
            if method == "GET":
                return ads_audiences_list()

        elif resource == "ads-pixel-stats":
            if method == "GET":
                return ads_pixel_stats()

        elif resource == "ad-ai-review":
            if method == "POST":
                return ad_ai_review(data)

        elif resource == "ad-creative-review":
            if method == "POST":
                return ad_creative_review(data)

        elif resource == "posts":
            if method == "GET":
                return list_posts()
            if method == "POST" and item_id == "refresh":
                return refresh_posts(data)

        elif resource == "fb-scheduled":
            if method == "GET":
                return fb_scheduled_posts()

        elif resource == "ad-insights":
            if method == "POST":
                return ad_account_insights(data)

        elif resource == "catalog":
            if method == "GET":
                return list_catalog()
            if method == "POST":
                return create_catalog(data)
            if method == "PUT" and item_id:
                return update_catalog(item_id, data)
            if method == "DELETE" and item_id:
                return delete_catalog(item_id)

        elif resource == "track":
            if method == "POST":
                return create_tracked(data)

        elif resource == "track-categories":
            if method == "POST":
                return track_categories(data)

        elif resource == "discover":
            if method == "POST":
                return discover_category(data)

        elif resource == "search-competitors":
            if method == "POST":
                return search_competitors_api(data)

        elif resource == "config":
            if method == "GET":
                return config_status()
            if method == "POST":
                return set_config(data)

        elif resource == "changes":
            if method == "GET":
                return list_changes(args.get("competitorId"), args.get("type"))

        return _json({"error": f"נתיב לא נתמך: {method} /{resource}"}, 404)

    except Exception as e:
        return _json({"error": str(e)}, 500)

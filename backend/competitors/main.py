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
import json as _jsonlib
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

MODEL_FIELDS = {"name", "myUrl", "myPrice", "status"}
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
        r = requests.get(url, headers=SCAN_HEADERS, timeout=20)
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
    fields = _pick(data, {"name", "url"})
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


def scan_model(mid):
    """סורק מחיר אמיתי: הדף שלי + כל המתחרים, מעדכן ב-Firestore ומחזיר את התוצאה."""
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
    for s in db.collection("priceSources").where("modelId", "==", mid).stream():
        sd = s.to_dict()
        res = fetch_price(sd.get("url", ""))
        s.reference.update({
            "lastPrice": res["price"], "lastScanAt": now, "lastStatus": res.get("status"),
        })
        comps.append({"id": s.id, "name": sd.get("name"),
                      "price": res["price"], "status": res.get("status")})
    result["competitors"] = comps
    return _json(result)


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

        elif resource == "changes":
            if method == "GET":
                return list_changes(args.get("competitorId"), args.get("type"))

        return _json({"error": f"נתיב לא נתמך: {method} /{resource}"}, 404)

    except Exception as e:
        return _json({"error": str(e)}, 500)

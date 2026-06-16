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
CUSTOMER_FIELDS = {"name", "company", "phone", "email", "status", "notes", "lastContact"}
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


def scan_all():
    """סורק את כל הדגמים — מופעל ידנית ('סרוק הכל') ובתזמון יומי (Cloud Scheduler)."""
    ids = [s.id for s in db.collection("models").stream()]
    for mid in ids:
        try:
            scan_model(mid)
        except Exception:
            pass
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
                          "fbPageToken", "fbPageId", "igUserId", "adAccountId", "adsToken"})
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    db.collection("config").document("settings").set(fields, merge=True)
    # מחזירים רק האם המפתחות מוגדרים — בלי לחשוף אותם
    return _json({"ok": True, "serpConfigured": bool(_get_config("serpApiKey")),
                  "ideogramConfigured": bool(_get_config("ideogramKey")),
                  "metaConfigured": bool(_get_config("metaToken")),
                  "fbConfigured": bool(_get_config("fbPageToken") and _get_config("fbPageId"))})


def config_status():
    return _json({"serpConfigured": bool(_get_config("serpApiKey")),
                  "serpProvider": _get_config("serpProvider", "serpapi"),
                  "ideogramConfigured": bool(_get_config("ideogramKey")),
                  "metaConfigured": bool(_get_config("metaToken")),
                  "fbConfigured": bool(_get_config("fbPageToken") and _get_config("fbPageId")),
                  "igConfigured": bool(_get_config("fbPageToken") and _get_config("igUserId")),
                  "adsConfigured": bool(_get_config("adAccountId"))})


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
    token = _resolve_page_token(token, page)
    try:
        if image_b64:
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
                    "createdAt": firestore.SERVER_TIMESTAMP,
                    "likes": 0, "comments": 0, "shares": 0, "reactions": 0,
                }, merge=True)
            except Exception:
                pass
        return _json({"ok": True, "postId": post_id,
                      "url": f"https://www.facebook.com/{post_id}" if post_id else None})
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


def list_posts():
    """כל הפוסטים שפורסמו דרך האפליקציה + המדדים השמורים שלהם (לדוחות/למידה)."""
    docs = db.collection("posts").order_by("createdAt", direction=firestore.Query.DESCENDING).stream()
    return _json({"posts": [_ts_to_iso(_doc_to_dict(d)) for d in docs]})


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
        return _json({"name": name, "price": price, "description": desc, "image": image, "url": url})
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

CATALOG_FIELDS = {"sku", "name", "usdPrice", "ilsManual", "shippingPct", "desiredProfit"}


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
                return scan_all()

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

        elif resource == "customers":
            if method == "GET":
                return list_customers()
            if method == "POST":
                return create_customer(data)
            if method == "PUT" and item_id:
                return update_customer(item_id, data)
            if method == "DELETE" and item_id:
                return delete_customer(item_id)

        elif resource == "posts":
            if method == "GET":
                return list_posts()
            if method == "POST" and item_id == "refresh":
                return refresh_posts(data)

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

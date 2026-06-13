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
import functions_framework
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

        elif resource == "changes":
            if method == "GET":
                return list_changes(args.get("competitorId"), args.get("type"))

        return _json({"error": f"נתיב לא נתמך: {method} /{resource}"}, 404)

    except Exception as e:
        return _json({"error": str(e)}, 500)

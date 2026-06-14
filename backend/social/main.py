"""
דנא ציוד אינסטלציה — Social Media & Campaign Intelligence Backend
Google Cloud Function לניטור מתחרים ברשתות חברתיות, יצירת קמפיינים, ופרסום אוטומטי.

Entry point: social_api
Runtime: Python 3.12
ALLOWED_ORIGIN — משתנה סביבה לשליטה על CORS

Firestore Collections:
  socialConfig   { facebookPageId, facebookPageName, facebookAccessToken,
                   instagramAccountId, platforms, updatedAt }
  competitorAds  { adId, advertiserId, advertiserName, adTitle, adBody,
                   adDescription, adSnapshotUrl, startDate, endDate,
                   impressionsLower, impressionsUpper, spend, platforms,
                   platform, languages, searchTerm, countries, scannedAt }
  campaigns      { title, platform, objective, content, hashtags, seoTags,
                   imagePrompt, budget, targetAudience, status, notes,
                   callToAction, adType, mediaUrl, mediaType,
                   scheduledAt, publishedAt, postId, postUrl,
                   createdAt, updatedAt }

Routes:
  GET|POST  /config          — הגדרות חשבונות Meta
  GET       /ads             — רשימת מודעות מתחרים
  POST      /scan-ads        — סריקת Facebook Ads Library
  GET       /campaigns       — רשימת קמפיינים
  POST      /campaigns       — יצירת קמפיין
  PUT       /campaigns/{id}  — עדכון קמפיין
  DELETE    /campaigns/{id}  — מחיקת קמפיין
  POST      /publish         — פרסום לפייסבוק / אינסטגרם
  GET       /export          — ייצוא JSON מלא
"""

import os
from collections import Counter
from datetime import datetime, timezone
import functions_framework
import requests
from google.cloud import firestore

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

CORS = {
    "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-API-Key",
}

db = firestore.Client()

FB_API = "https://graph.facebook.com/v19.0"
FB_ADS_ARCHIVE = f"{FB_API}/ads_archive"


# ── helpers ──────────────────────────────────────────────────────────────

def _doc_to_dict(doc):
    d = doc.to_dict() or {}
    d["id"] = doc.id
    return d


def _ts_to_iso(d):
    for k, v in list(d.items()):
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
    return d


def _pick(data, allowed):
    return {k: v for k, v in (data or {}).items() if k in allowed}


def _json(body, status=200):
    return (body, status, {**CORS, "Content-Type": "application/json"})


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _get_cfg(key=""):
    doc = db.collection("socialConfig").document("main").get()
    cfg = (doc.to_dict() or {}) if doc.exists else {}
    return cfg.get(key, "") if key else cfg


# ════════════════════ CONFIG ════════════════════

CONFIG_FIELDS = {
    "facebookPageId", "facebookPageName", "facebookAccessToken",
    "instagramAccountId", "platforms",
}


def get_config():
    cfg = _get_cfg()
    token = cfg.get("facebookAccessToken", "")
    return _json({
        "configured": bool(token),
        "facebookPageId": cfg.get("facebookPageId", ""),
        "facebookPageName": cfg.get("facebookPageName", ""),
        "instagramAccountId": cfg.get("instagramAccountId", ""),
        "platforms": cfg.get("platforms", ["facebook", "instagram"]),
        "tokenPreview": (f"...{token[-6:]}" if len(token) > 6 else ("מוגדר" if token else "")),
    })


def set_config(data):
    fields = _pick(data, CONFIG_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    db.collection("socialConfig").document("main").set(fields, merge=True)
    return _json({"ok": True, "configured": bool(fields.get("facebookAccessToken") or _get_cfg("facebookAccessToken"))})


# ════════════════════ COMPETITOR ADS ════════════════════

def list_ads(platform=None, advertiser=None, search_term=None, limit=100):
    col = db.collection("competitorAds")
    query = col.order_by("scannedAt", direction=firestore.Query.DESCENDING).limit(limit)
    ads = [_ts_to_iso(_doc_to_dict(d)) for d in query.stream()]
    if platform:
        ads = [a for a in ads if platform in (a.get("platforms") or [a.get("platform", "")])]
    if advertiser:
        ads = [a for a in ads if advertiser.lower() in (a.get("advertiserName") or "").lower()]
    if search_term:
        q = search_term.lower()
        ads = [a for a in ads if q in (a.get("adBody") or "").lower()
                              or q in (a.get("adTitle") or "").lower()
                              or q in (a.get("searchTerm") or "").lower()]
    return _json({"ads": ads, "count": len(ads)})


def scan_ads(data):
    """סריקת Facebook Ads Library API — מחפשת מודעות פעילות לפי מונחי חיפוש."""
    token = _get_cfg("facebookAccessToken")
    if not token:
        return _json({"error": "לא הוגדר Facebook Access Token — הגדר תחת הגדרות"}, 400)

    search_terms = (data or {}).get("searchTerms", [])
    if isinstance(search_terms, str):
        search_terms = [t.strip() for t in search_terms.split(",") if t.strip()]
    countries = (data or {}).get("countries", ["IL"])
    ad_type = (data or {}).get("adType", "ALL")
    limit = min(int((data or {}).get("limit", 50)), 200)

    if not search_terms:
        return _json({"error": "יש לספק לפחות מונח חיפוש אחד (searchTerms)"}, 400)

    all_results = []
    errors = []

    for term in search_terms[:5]:
        try:
            params = {
                "search_terms": term,
                "ad_reached_countries": countries,
                "ad_type": ad_type,
                "fields": (
                    "id,ad_creation_time,ad_delivery_start_time,ad_delivery_stop_time,"
                    "ad_snapshot_url,page_id,page_name,impressions,spend,currency,"
                    "ad_creative_bodies,ad_creative_link_titles,ad_creative_link_descriptions,"
                    "languages,publisher_platforms"
                ),
                "limit": limit,
                "access_token": token,
            }
            r = requests.get(FB_ADS_ARCHIVE, params=params, timeout=30)
            result = r.json()

            if "error" in result:
                errors.append(f"שגיאת API עבור '{term}': {result['error'].get('message', 'שגיאה לא ידועה')}")
                continue

            ads_data = result.get("data", [])
            now = firestore.SERVER_TIMESTAMP
            saved = 0

            for ad in ads_data:
                ad_id = ad.get("id") or db.collection("_").document().id
                bodies = ad.get("ad_creative_bodies") or []
                titles = ad.get("ad_creative_link_titles") or []
                descs = ad.get("ad_creative_link_descriptions") or []
                imps = ad.get("impressions") or {}
                platforms = ad.get("publisher_platforms") or ["facebook"]

                db.collection("competitorAds").document(ad_id).set({
                    "adId": ad_id,
                    "advertiserId": ad.get("page_id", ""),
                    "advertiserName": ad.get("page_name", ""),
                    "adTitle": titles[0] if titles else "",
                    "adBody": bodies[0] if bodies else "",
                    "adDescription": descs[0] if descs else "",
                    "adSnapshotUrl": ad.get("ad_snapshot_url", ""),
                    "startDate": ad.get("ad_delivery_start_time", ""),
                    "endDate": ad.get("ad_delivery_stop_time", ""),
                    "creationTime": ad.get("ad_creation_time", ""),
                    "impressionsLower": imps.get("lower_bound", "0"),
                    "impressionsUpper": imps.get("upper_bound", "0"),
                    "spend": ad.get("spend", {}),
                    "platforms": platforms,
                    "platform": platforms[0] if platforms else "facebook",
                    "languages": ad.get("languages", []),
                    "searchTerm": term,
                    "countries": countries,
                    "scannedAt": now,
                }, merge=True)
                saved += 1

            all_results.append({"searchTerm": term, "found": len(ads_data), "saved": saved})

        except requests.exceptions.Timeout:
            errors.append(f"timeout עבור '{term}'")
        except Exception as e:
            errors.append(f"שגיאה עבור '{term}': {str(e)}")

    return _json({
        "scanned": len(all_results),
        "results": all_results,
        "totalFound": sum(r.get("found", 0) for r in all_results),
        "errors": errors,
        "scannedAt": _now_iso(),
    })


# ════════════════════ CAMPAIGNS ════════════════════

CAMPAIGN_FIELDS = {
    "title", "platform", "objective", "content", "hashtags",
    "seoTags", "imagePrompt", "budget", "targetAudience",
    "status", "notes", "scheduledAt", "adType", "callToAction",
    "mediaUrl", "mediaType", "publishedAt", "postId", "postUrl",
}


def list_campaigns():
    docs = db.collection("campaigns").order_by(
        "createdAt", direction=firestore.Query.DESCENDING
    ).limit(100).stream()
    return _json({"campaigns": [_ts_to_iso(_doc_to_dict(d)) for d in docs]})


def create_campaign(data):
    fields = _pick(data, CAMPAIGN_FIELDS)
    if not fields.get("title"):
        return _json({"error": "שדה 'title' הוא חובה"}, 400)
    fields.setdefault("platform", "facebook")
    fields.setdefault("status", "draft")
    fields["createdAt"] = firestore.SERVER_TIMESTAMP
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("campaigns").document()
    ref.set(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())), 201)


def update_campaign(cid, data):
    fields = _pick(data, CAMPAIGN_FIELDS)
    if not fields:
        return _json({"error": "אין שדות לעדכון"}, 400)
    fields["updatedAt"] = firestore.SERVER_TIMESTAMP
    ref = db.collection("campaigns").document(cid)
    if not ref.get().exists:
        return _json({"error": "קמפיין לא נמצא"}, 404)
    ref.update(fields)
    return _json(_ts_to_iso(_doc_to_dict(ref.get())))


def delete_campaign(cid):
    ref = db.collection("campaigns").document(cid)
    if not ref.get().exists:
        return _json({"error": "קמפיין לא נמצא"}, 404)
    ref.delete()
    return _json({"deleted": cid})


# ════════════════════ PUBLISH ════════════════════

def publish_post(data):
    """פרסום פוסט לפייסבוק ו/או אינסטגרם דרך Meta Graph API."""
    token = _get_cfg("facebookAccessToken")
    if not token:
        return _json({"error": "לא הוגדר Facebook Access Token — הגדר בהגדרות"}, 400)

    cfg = _get_cfg()
    campaign_id = (data or {}).get("campaignId", "")
    content = (data or {}).get("content", "")
    media_url = (data or {}).get("mediaUrl", "")
    platform = (data or {}).get("platform", "facebook")
    page_id = (data or {}).get("pageId") or cfg.get("facebookPageId", "")
    ig_id = (data or {}).get("instagramAccountId") or cfg.get("instagramAccountId", "")

    if not content:
        return _json({"error": "תוכן הפוסט הוא חובה"}, 400)
    if platform in ("facebook", "both") and not page_id:
        return _json({"error": "לא הוגדר Facebook Page ID — הגדר בהגדרות"}, 400)

    results = {}
    errors = []

    # ── Facebook ──
    if platform in ("facebook", "both") and page_id:
        try:
            if media_url:
                r = requests.post(f"{FB_API}/{page_id}/photos", data={
                    "url": media_url, "caption": content, "access_token": token,
                }, timeout=30)
            else:
                r = requests.post(f"{FB_API}/{page_id}/feed", data={
                    "message": content, "access_token": token,
                }, timeout=30)
            res = r.json()
            if "error" in res:
                errors.append(f"Facebook: {res['error'].get('message', 'שגיאה')}")
            else:
                post_id = res.get("post_id") or res.get("id", "")
                results["facebook"] = {
                    "postId": post_id,
                    "postUrl": f"https://www.facebook.com/{post_id.replace('_', '/posts/')}" if post_id else "",
                    "success": True,
                }
        except Exception as e:
            errors.append(f"Facebook error: {str(e)}")

    # ── Instagram (נדרשת תמונה) ──
    if platform in ("instagram", "both") and ig_id:
        if not media_url:
            errors.append("Instagram: נדרשת תמונה לפרסום")
        else:
            try:
                r1 = requests.post(f"{FB_API}/{ig_id}/media", data={
                    "image_url": media_url, "caption": content, "access_token": token,
                }, timeout=30)
                r1_data = r1.json()
                if "error" in r1_data:
                    errors.append(f"Instagram media: {r1_data['error'].get('message', 'שגיאה')}")
                else:
                    creation_id = r1_data.get("id", "")
                    r2 = requests.post(f"{FB_API}/{ig_id}/media_publish", data={
                        "creation_id": creation_id, "access_token": token,
                    }, timeout=30)
                    r2_data = r2.json()
                    if "error" in r2_data:
                        errors.append(f"Instagram publish: {r2_data['error'].get('message', 'שגיאה')}")
                    else:
                        results["instagram"] = {"postId": r2_data.get("id", ""), "success": True}
            except Exception as e:
                errors.append(f"Instagram error: {str(e)}")

    # ── עדכון קמפיין ב-Firestore ──
    if campaign_id and results:
        now = firestore.SERVER_TIMESTAMP
        upd = {"status": "published", "publishedAt": now, "updatedAt": now}
        if "facebook" in results:
            upd["postId"] = results["facebook"]["postId"]
            upd["postUrl"] = results["facebook"].get("postUrl", "")
        db.collection("campaigns").document(campaign_id).update(upd)

    return _json({
        "success": bool(results),
        "results": results,
        "errors": errors,
        "publishedAt": _now_iso(),
    })


# ════════════════════ EXPORT ════════════════════

def export_all():
    """ייצוא JSON מלא: מודעות מתחרים, קמפיינים, סיכום SEO ואסטרטגיה."""
    ads = [_ts_to_iso(_doc_to_dict(d))
           for d in db.collection("competitorAds")
                      .order_by("scannedAt", direction=firestore.Query.DESCENDING)
                      .limit(500).stream()]

    campaigns = [_ts_to_iso(_doc_to_dict(d))
                 for d in db.collection("campaigns")
                            .order_by("createdAt", direction=firestore.Query.DESCENDING)
                            .limit(200).stream()]

    cfg = _get_cfg()
    cfg.pop("facebookAccessToken", None)  # לא לחשוף את ה-token

    return _json({
        "exportedAt": _now_iso(),
        "meta": {
            "version": "1.0",
            "source": "dna-tools.co.il",
            "description": "Social Media Intelligence Export — DNA Tools",
        },
        "config": cfg,
        "competitorAds": {
            "count": len(ads),
            "data": ads,
        },
        "campaigns": {
            "count": len(campaigns),
            "data": campaigns,
            "statusBreakdown": dict(Counter(c.get("status", "draft") for c in campaigns)),
            "platformBreakdown": dict(Counter(c.get("platform", "facebook") for c in campaigns)),
        },
        "seoSummary": _build_seo_summary(campaigns),
        "adStrategySummary": _build_ad_strategy(ads),
        "publishingData": {
            "published": [c for c in campaigns if c.get("status") == "published"],
            "scheduled": [c for c in campaigns if c.get("status") == "scheduled"],
            "drafts": [c for c in campaigns if c.get("status") == "draft"],
        },
    })


def _build_seo_summary(campaigns):
    all_tags, all_hashtags = [], []
    for c in campaigns:
        tags = c.get("seoTags") or []
        if isinstance(tags, list):
            all_tags.extend(tags)
        elif isinstance(tags, str):
            all_tags.extend(t.strip() for t in tags.split(",") if t.strip())
        htags = c.get("hashtags") or []
        if isinstance(htags, list):
            all_hashtags.extend(htags)
        elif isinstance(htags, str):
            all_hashtags.extend(h.strip() for h in htags.split() if h.strip().startswith("#"))
    return {
        "topSeoTags": [{"tag": t, "count": c} for t, c in Counter(all_tags).most_common(20)],
        "topHashtags": [{"hashtag": h, "count": c} for h, c in Counter(all_hashtags).most_common(20)],
    }


def _build_ad_strategy(ads):
    advertisers: dict = {}
    for ad in ads:
        name = ad.get("advertiserName") or "לא ידוע"
        if name not in advertisers:
            advertisers[name] = {"name": name, "adsCount": 0, "platforms": set()}
        advertisers[name]["adsCount"] += 1
        for p in (ad.get("platforms") or [ad.get("platform", "facebook")]):
            advertisers[name]["platforms"].add(p)

    platform_counter: list = []
    for ad in ads:
        platform_counter.extend(ad.get("platforms") or [ad.get("platform", "facebook")])

    return {
        "totalAds": len(ads),
        "uniqueAdvertisers": len(advertisers),
        "topAdvertisers": sorted(
            [{"name": k, "adsCount": v["adsCount"], "platforms": list(v["platforms"])}
             for k, v in advertisers.items()],
            key=lambda x: x["adsCount"], reverse=True
        )[:10],
        "platformBreakdown": dict(Counter(platform_counter)),
        "insights": _generate_insights(ads),
    }


def _generate_insights(ads):
    if not ads:
        return []
    # מחלץ תובנות בסיסיות מהנתונים
    bodies = [a.get("adBody", "") for a in ads if a.get("adBody")]
    total = len(bodies)
    has_emoji = sum(1 for b in bodies if any(ord(c) > 127 for c in b[:20]))
    has_question = sum(1 for b in bodies if "?" in b or "؟" in b)
    avg_len = int(sum(len(b) for b in bodies) / total) if total else 0
    return [
        f"{has_emoji}/{total} מודעות מתחילות עם אמוג׳י — אפקטיבי לעצירת גלילה",
        f"{has_question}/{total} מודעות משתמשות בשאלות כדי למשוך מעורבות",
        f"אורך טקסט ממוצע: {avg_len} תווים",
    ]


# ════════════════════ ROUTER ════════════════════

@functions_framework.http
def social_api(request):
    if request.method == "OPTIONS":
        return ("", 204, CORS)

    parts = [p for p in (request.path or "").split("/") if p]
    resource = parts[0] if parts else ""
    item_id = parts[1] if len(parts) > 1 else None
    method = request.method
    data = request.get_json(silent=True) or {}
    args = request.args

    try:
        if resource == "config":
            if method == "GET":
                return get_config()
            if method == "POST":
                return set_config(data)

        elif resource == "ads":
            if method == "GET":
                return list_ads(
                    platform=args.get("platform"),
                    advertiser=args.get("advertiser"),
                    search_term=args.get("q"),
                    limit=int(args.get("limit", 100)),
                )

        elif resource == "scan-ads":
            if method == "POST":
                return scan_ads(data)

        elif resource == "campaigns":
            if method == "GET":
                return list_campaigns()
            if method == "POST":
                return create_campaign(data)
            if method == "PUT" and item_id:
                return update_campaign(item_id, data)
            if method == "DELETE" and item_id:
                return delete_campaign(item_id)

        elif resource == "publish":
            if method == "POST":
                return publish_post(data)

        elif resource == "export":
            if method == "GET":
                return export_all()

        return _json({"error": f"נתיב לא נתמך: {method} /{resource}"}, 404)

    except Exception as e:
        return _json({"error": str(e)}, 500)

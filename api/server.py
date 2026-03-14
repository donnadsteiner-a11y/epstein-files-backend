"""
Flask REST API — Serves document metadata, persons, timeline events,
and monitor status to the frontend HTML application.
"""
import os
import sys
import json
import logging

from flask import Flask, request, jsonify, send_from_directory, redirect
from flask_cors import CORS

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    API_HOST, API_PORT, API_DEBUG, CORS_ORIGINS, STATIC_DIR,
    S3_PUBLIC_URL, DOJ_DATA_SET_URLS, DOJ_BASE
)
from db.database import (
    init_db, get_documents, get_document_count, get_persons,
    get_timeline, get_monitor_log, get_stats, get_db, query_one, query_rows, query_val
)

app = Flask(__name__, static_folder=STATIC_DIR)
CORS(app, origins=CORS_ORIGINS)
from auth_routes import auth_bp
app.register_blueprint(auth_bp)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

# Initialize database on startup
init_db()

@app.route("/")
def root():
    return jsonify({
        "ok": True,
        "service": "epstein-files-api",
        "health": "/api/health",
        "stats": "/api/stats"
    })

@app.route("/static/<path:path>")
def serve_static(path):
    return send_from_directory(STATIC_DIR, path)

@app.route("/api/stats")
def api_stats():
    stats = get_stats()
    return jsonify(stats)

@app.route("/api/documents")
def api_documents():
    file_type = request.args.get("type", "all")
    dataset_id = request.args.get("dataset", type=int)
    source = request.args.get("source")
    sort = request.args.get("sort", "date_desc")
    search = request.args.get("q")
    limit = min(request.args.get("limit", 50, type=int), 500)
    offset = request.args.get("offset", 0, type=int)

    docs = get_documents(
        file_type=file_type, dataset_id=dataset_id, source=source,
        sort=sort, search=search, limit=limit, offset=offset
    )
    total = get_document_count(file_type=file_type, dataset_id=dataset_id)

    return jsonify({
        "documents": docs,
        "total": total,
        "limit": limit,
        "offset": offset,
    })

@app.route("/api/file/<file_id>")
def api_file_redirect(file_id):
    with get_db() as conn:
        doc = query_one(conn,
            "SELECT s3_url, source_url FROM documents WHERE file_id = %s", (file_id,))

    if doc and doc.get("s3_url"):
        return redirect(doc["s3_url"])
    elif doc and doc.get("source_url"):
        return redirect(doc["source_url"])
    else:
        return jsonify({"error": "File not found"}), 404

@app.route("/api/persons")
def api_persons():
    category = request.args.get("category", "all")
    search = request.args.get("q")
    sort = request.args.get("sort", "mentions_desc")
    limit = min(request.args.get("limit", 100, type=int), 500)

    persons = get_persons(category=category, search=search, sort=sort, limit=limit)
    return jsonify({"persons": persons, "total": len(persons)})

@app.route("/api/persons/<name>/documents")
def api_person_documents(name):
    limit = min(request.args.get("limit", 50, type=int), 200)

    with get_db() as conn:
        person = query_one(conn,
            "SELECT id FROM persons WHERE name = %s", (name,))

        if not person:
            return jsonify({"error": "Person not found"}), 404

        docs = query_rows(conn, """
            SELECT d.* FROM documents d
            JOIN document_persons dp ON d.id = dp.document_id
            WHERE dp.person_id = %s
            ORDER BY d.date_on_doc DESC
            LIMIT %s
        """, (person["id"], limit))

    return jsonify({"documents": docs, "person": name})

@app.route("/api/timeline")
def api_timeline():
    person = request.args.get("person", "all")
    event_type = request.args.get("type", "all")
    year_start = request.args.get("year_start", type=int)
    year_end = request.args.get("year_end", type=int)
    search = request.args.get("q")
    limit = min(request.args.get("limit", 200, type=int), 500)

    events = get_timeline(
        person=person, event_type=event_type,
        year_start=year_start, year_end=year_end,
        search=search, limit=limit
    )
    return jsonify({"events": events, "total": len(events)})

@app.route("/api/flights")
def api_flights():
    flights = [
        {"from": "Teterboro, NJ", "to": "Palm Beach, FL", "freq": "Regular", "aircraft": "Boeing 727 / Gulfstream", "alias": "Lolita Express"},
        {"from": "Palm Beach, FL", "to": "Little St. James, USVI", "freq": "Regular", "aircraft": "Boeing 727 / Gulfstream", "alias": "Lolita Express"},
        {"from": "Teterboro, NJ", "to": "Little St. James, USVI", "freq": "Frequent", "aircraft": "Boeing 727", "alias": "Lolita Express"},
        {"from": "New York, NY", "to": "Paris, France", "freq": "Occasional", "aircraft": "Boeing 727", "alias": "Lolita Express"},
        {"from": "Palm Beach, FL", "to": "Zorro Ranch, NM", "freq": "Occasional", "aircraft": "Gulfstream", "alias": ""},
        {"from": "Necker Island, BVI", "to": "Little St. James, USVI", "freq": "Documented", "aircraft": "Various", "alias": ""},
    ]
    return jsonify({"flights": flights})

@app.route("/api/locations")
def api_locations():
    locations = [
        {"name": "Manhattan Townhouse", "address": "9 E 71st St, New York, NY", "lat": 40.7712, "lng": -73.9645, "type": "property"},
        {"name": "Palm Beach Estate", "address": "358 El Brillo Way, Palm Beach, FL", "lat": 26.7054, "lng": -80.0384, "type": "property"},
        {"name": "Little St. James Island", "address": "US Virgin Islands", "lat": 18.3000, "lng": -64.8252, "type": "property"},
        {"name": "Great St. James Island", "address": "US Virgin Islands", "lat": 18.3167, "lng": -64.8333, "type": "property"},
        {"name": "Zorro Ranch", "address": "Stanley, NM", "lat": 35.1464, "lng": -105.9619, "type": "property"},
        {"name": "Paris Apartment", "address": "Avenue Foch, Paris, France", "lat": 48.8738, "lng": 2.2870, "type": "property"},
        {"name": "Metropolitan Correctional Center", "address": "150 Park Row, New York, NY", "lat": 40.7128, "lng": -74.0006, "type": "legal"},
        {"name": "Teterboro Airport", "address": "Teterboro, NJ", "lat": 40.8501, "lng": -74.0608, "type": "travel"},
    ]
    return jsonify({"locations": locations})

@app.route("/api/datasets")
def api_datasets():
    datasets = []
    for ds_id in range(1, 13):
        count = get_document_count(dataset_id=ds_id)
        datasets.append({
            "id": ds_id,
            "label": f"Data Set {ds_id}",
            "doj_url": f"{DOJ_BASE}{DOJ_DATA_SET_URLS.get(ds_id, '')}",
            "documents_downloaded": count,
        })
    return jsonify({"datasets": datasets})

@app.route("/api/monitor")
def api_monitor():
    limit = min(request.args.get("limit", 50, type=int), 200)
    log = get_monitor_log(limit=limit)
    return jsonify({"log": log, "total": len(log)})

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Search query required"}), 400
    docs = get_documents(search=q, limit=20)
    persons = get_persons(search=q, limit=10)
    events = get_timeline(search=q, limit=20)
    return jsonify({
        "query": q,
        "documents": docs,
        "persons": persons,
        "timeline_events": events,
    })

@app.route("/api/scan-progress")
def api_scan_progress():
    """Show EFTA scanning progress per dataset."""
    try:
        from crawler.url_generator import get_scan_summary
        summary = get_scan_summary()
        total_found = sum(s["files_found"] for s in summary)
        total_complete = sum(1 for s in summary if s["completed"])
        return jsonify({
            "datasets": summary,
            "total_files_found": total_found,
            "datasets_completed": total_complete,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/pending")
def api_pending():
    """Check how many URLs are still pending download."""
    with get_db() as conn:
        total = query_val(conn, "SELECT COUNT(*) FROM scraped_urls")
        pending = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE downloaded = 0")
        downloaded = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE downloaded = 1")
    return jsonify({"total_urls": total, "pending": pending, "downloaded": downloaded})

@app.route("/api/reset-failed")
def api_reset_failed():
    """Reset all non-downloaded URLs so the downloader retries them."""
    with get_db() as conn:
        cur = conn.cursor()
        # Reset URLs that were scraped but never successfully downloaded to S3
        cur.execute("""
            UPDATE scraped_urls SET downloaded = 0
            WHERE downloaded = 1
            AND url NOT IN (SELECT source_url FROM documents WHERE source_url IS NOT NULL)
        """)
        reset_count = cur.rowcount
        cur.close()
    return jsonify({"reset": reset_count, "message": f"Reset {reset_count} URLs for retry"})

@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok", "version": "2.1.0"})

if __name__ == "__main__":
    logger.info(f"Starting API server on {API_HOST}:{API_PORT}")
    app.run(host=API_HOST, port=API_PORT, debug=API_DEBUG)

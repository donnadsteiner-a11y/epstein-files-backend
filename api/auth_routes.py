"""
api/auth_routes.py — DocketZero Authentication Blueprint
"""

import os
import re
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import timedelta

from flask import Blueprint, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    jwt_required,
    get_jwt_identity,
)
import psycopg2
import psycopg2.errors

logger = logging.getLogger("api")

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")
CORS(auth_bp, origins=[
    "https://docketzero.com",
    "http://docketzero.com",
    "https://www.docketzero.com",
    "http://localhost",
    "http://localhost:5000",
    "http://localhost:8080",
    "http://127.0.0.1",
    "http://127.0.0.1:5000",
])


# ─── DB ──────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ─── HELPERS ─────────────────────────────────────────────────────────
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email))

def fmt_dt(dt):
    return dt.isoformat() if dt else None


# ─── EMAIL ───────────────────────────────────────────────────────────
def send_welcome_email(to_email: str, first_name: str = ""):
    try:
        smtp_host = os.environ.get("SMTP_HOST", "mail.docketzero.com")
        smtp_port = int(os.environ.get("SMTP_PORT", 587))
        smtp_user = os.environ.get("SMTP_USER", "support@docketzero.com")
        smtp_pass = os.environ.get("SMTP_PASS", "")

        greeting = f"Hi {first_name}," if first_name else "Welcome,"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Welcome to DocketZero"
        msg["From"] = f"DocketZero <{smtp_user}>"
        msg["To"] = to_email

        text = f"""{greeting}

Your DocketZero account has been created.

You now have free access to:
- The Epstein Files (6,788+ documents from DOJ / EFTA releases)
- Entity search, saved searches, bookmarks, and reading history
- Dataset downloads (ZIP archives of raw DOJ files)

Sign in at: https://docketzero.com

— The DocketZero Team
"""

        html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  body{{margin:0;padding:0;background:#eef3f8;font-family:Inter,Arial,sans-serif}}
  .wrap{{max-width:560px;margin:40px auto;background:#fff;border:1px solid #dbe4ec;border-radius:16px;overflow:hidden}}
  .hdr{{background:linear-gradient(180deg,#edf3f8,#e3ebf3);padding:28px 32px;border-bottom:1px solid #cad6e2}}
  .hdr-title{{font-size:22px;font-weight:800;color:#0f1720;margin-top:10px}}
  .hdr-title span{{color:#c99a3c}}
  .body{{padding:32px}}
  .body p{{font-size:15px;color:#5f7387;line-height:1.7;margin:0 0 16px}}
  .body strong{{color:#0f1720}}
  .features{{background:#f5f8fb;border:1px solid #dbe4ec;border-radius:12px;padding:18px 20px;margin:20px 0}}
  .feature{{display:flex;align-items:flex-start;gap:10px;margin-bottom:10px;font-size:14px;color:#0f1720}}
  .feature:last-child{{margin-bottom:0}}
  .check{{color:#2f8f53;font-weight:700}}
  .cta{{display:block;text-align:center;margin:24px 0 8px;padding:14px 24px;background:#c99a3c;color:#fff;text-decoration:none;border-radius:10px;font-size:15px;font-weight:700}}
  .footer{{padding:20px 32px;border-top:1px solid #dbe4ec;font-size:12px;color:#5f7387;text-align:center;line-height:1.6}}
  .footer a{{color:#5f7387}}
  .notice{{margin:20px 0 0;padding:14px 16px;background:#fff8f0;border:1px solid #f0d9b8;border-left:3px solid #c99a3c;border-radius:10px;font-size:13px;color:#5f7387;line-height:1.6}}
</style></head>
<body><div class="wrap">
  <div class="hdr">
    <div class="hdr-title">DocketZero<span>.</span></div>
    <div style="font-size:13px;color:#5f7387;margin-top:4px">Evidence-first research platform</div>
  </div>
  <div class="body">
    <p>{greeting}</p>
    <p>Your account is confirmed. You now have <strong>free access</strong> to:</p>
    <div class="features">
      <div class="feature"><span class="check">✓</span> Epstein Files — 6,788+ documents from DOJ / EFTA releases</div>
      <div class="feature"><span class="check">✓</span> Entity search, timelines &amp; relationship views</div>
      <div class="feature"><span class="check">✓</span> Saved searches, bookmarks &amp; reading history</div>
      <div class="feature"><span class="check">✓</span> Dataset ZIP downloads</div>
    </div>
    <a class="cta" href="https://docketzero.com/dashboard.html">Go to My Dashboard →</a>
    <div class="notice">
      <strong style="color:#0f1720">Reminder:</strong> DocketZero is a public research platform. Your account data is never sold or shared.
    </div>
  </div>
  <div class="footer">
    © 2026 DocketZero &nbsp;·&nbsp; <a href="https://docketzero.com">docketzero.com</a><br>
    Questions? <a href="mailto:support@docketzero.com">support@docketzero.com</a>
  </div>
</div></body></html>"""

        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, to_email, msg.as_string())

        logger.info(f"Welcome email sent to {to_email}")

    except Exception as e:
        logger.warning(f"Failed to send welcome email to {to_email}: {e}")


# ─── REGISTER ────────────────────────────────────────────────────────
@auth_bp.route("/register", methods=["POST"])
def register():
    data       = request.get_json(silent=True) or {}
    email      = data.get("email", "").strip().lower()
    password   = data.get("password", "")
    first_name = data.get("first_name", "").strip()
    last_name  = data.get("last_name", "").strip()

    if not email or not valid_email(email):
        return jsonify({"error": "Valid email address required."}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters."}), 400

    pw_hash = generate_password_hash(password)

    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            """
            INSERT INTO users
                (email, password_hash, first_name, last_name, plan,
                 age_verified, disclaimer_accepted, created_at)
            VALUES (%s, %s, %s, %s, 'free', FALSE, FALSE, NOW())
            RETURNING id
            """,
            (email, pw_hash, first_name, last_name),
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.errors.UniqueViolation:
        return jsonify({"error": "An account with that email already exists."}), 409
    except Exception as e:
        logger.error(f"Registration error: {e}")
        return jsonify({"error": "Registration failed. Please try again."}), 500

    access_token  = create_access_token(identity=str(user_id), expires_delta=timedelta(hours=24))
    refresh_token = create_refresh_token(identity=str(user_id), expires_delta=timedelta(days=30))

    send_welcome_email(email, first_name)

    return jsonify({
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "user": {
            "id":                  user_id,
            "email":               email,
            "first_name":          first_name,
            "last_name":           last_name,
            "plan":                "free",
            "age_verified":        False,
            "disclaimer_accepted": False,
        },
    }), 201


# ─── LOGIN ───────────────────────────────────────────────────────────
@auth_bp.route("/login", methods=["POST"])
def login():
    data     = request.get_json(silent=True) or {}
    email    = data.get("email", "").strip().lower()
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400

    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            """
            SELECT id, email, password_hash, plan,
                   first_name, last_name,
                   age_verified, disclaimer_accepted
            FROM users WHERE email = %s
            """,
            (email,),
        )
        row = cur.fetchone()
    except Exception as e:
        logger.error(f"Login DB error: {e}")
        return jsonify({"error": "Login failed. Please try again."}), 500

    if not row or not check_password_hash(row[2], password):
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"error": "Invalid email or password."}), 401

    try:
        cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (row[0],))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

    access_token  = create_access_token(identity=str(row[0]), expires_delta=timedelta(hours=24))
    refresh_token = create_refresh_token(identity=str(row[0]), expires_delta=timedelta(days=30))

    return jsonify({
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "user": {
            "id":                  row[0],
            "email":               row[1],
            "plan":                row[3],
            "first_name":          row[4] or "",
            "last_name":           row[5] or "",
            "age_verified":        bool(row[6]),
            "disclaimer_accepted": bool(row[7]),
        },
    })


# ─── ME (GET / PATCH) ───────────────────────────────────────────────
@auth_bp.route("/me", methods=["GET", "PATCH"])
@jwt_required()
def me():
    user_id = get_jwt_identity()

    # ── PATCH — update age_verified / disclaimer_accepted / name ─────
    if request.method == "PATCH":
        data    = request.get_json(silent=True) or {}
        allowed = {"age_verified", "disclaimer_accepted", "first_name", "last_name"}
        updates = {k: v for k, v in data.items() if k in allowed}

        if not updates:
            return jsonify({"error": "No valid fields to update."}), 400

        set_parts = []
        values    = []
        for col, val in updates.items():
            set_parts.append(f"{col} = %s")
            values.append(val)
        values.append(user_id)

        try:
            conn = get_db()
            cur  = conn.cursor()
            cur.execute(
                f"UPDATE users SET {', '.join(set_parts)} WHERE id = %s",
                values,
            )
            conn.commit()
            cur.execute(
                """
                SELECT id, email, plan, first_name, last_name,
                       age_verified, disclaimer_accepted
                FROM users WHERE id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"PATCH /me error: {e}")
            return jsonify({"error": str(e)}), 500

        return jsonify({
            "id":                  row[0],
            "email":               row[1],
            "plan":                row[2],
            "first_name":          row[3] or "",
            "last_name":           row[4] or "",
            "age_verified":        bool(row[5]),
            "disclaimer_accepted": bool(row[6]),
        })

    # ── GET ───────────────────────────────────────────────────────────
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            """
            SELECT id, email, plan, first_name, last_name,
                   age_verified, disclaimer_accepted,
                   created_at, last_login,
                   notification_email, notification_inapp, data_retention_days
            FROM users WHERE id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not row:
        return jsonify({"error": "User not found."}), 404

    return jsonify({
        "id":                  row[0],
        "email":               row[1],
        "plan":                row[2],
        "first_name":          row[3] or "",
        "last_name":           row[4] or "",
        "age_verified":        bool(row[5]),
        "disclaimer_accepted": bool(row[6]),
        "created_at":          fmt_dt(row[7]),
        "last_login":          fmt_dt(row[8]),
        "notification_email":  row[9],
        "notification_inapp":  row[10],
        "data_retention_days": row[11],
    })


# ─── LOGOUT ──────────────────────────────────────────────────────────
@auth_bp.route("/logout", methods=["POST"])
@jwt_required()
def logout():
    return jsonify({"message": "Logged out successfully."})


# ─── TOKEN REFRESH ───────────────────────────────────────────────────
@auth_bp.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    user_id   = get_jwt_identity()
    new_token = create_access_token(identity=user_id, expires_delta=timedelta(hours=24))
    return jsonify({"access_token": new_token})


# ─── NOTIFICATION PREFERENCES ────────────────────────────────────────
@auth_bp.route("/notifications/preferences", methods=["PUT"])
@jwt_required()
def update_notification_prefs():
    user_id     = get_jwt_identity()
    data        = request.get_json(silent=True) or {}
    email_notif = bool(data.get("notification_email", True))
    inapp_notif = bool(data.get("notification_inapp", True))

    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET notification_email = %s, notification_inapp = %s
            WHERE id = %s
            """,
            (email_notif, inapp_notif, user_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

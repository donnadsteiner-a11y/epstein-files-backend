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
CORS(auth_bp, origins=["https://docketzero.com", "http://docketzero.com", "http://localhost"])


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
def send_welcome_email(to_email: str):
    try:
        smtp_host = os.environ.get("SMTP_HOST", "mail.docketzero.com")
        smtp_port = int(os.environ.get("SMTP_PORT", 587))
        smtp_user = os.environ.get("SMTP_USER", "support@docketzero.com")
        smtp_pass = os.environ.get("SMTP_PASS", "")

        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Welcome to DocketZero"
        msg["From"] = f"DocketZero <{smtp_user}>"
        msg["To"] = to_email

        text = f"""Welcome to DocketZero.

Your account has been created and you now have free access to:
- The Epstein Files (6,788 documents)
- Panama Papers (coming soon)
- Entity search, timelines, and relationship views

Sign in at any time: https://docketzero.com/login.html

If you have questions, reply to this email.

— The DocketZero Team
"""

        html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  body {{ margin:0; padding:0; background:#eef3f8; font-family:Inter,Arial,sans-serif; }}
  .wrap {{ max-width:560px; margin:40px auto; background:#ffffff; border:1px solid #dbe4ec; border-radius:16px; overflow:hidden; }}
  .header {{ background:linear-gradient(180deg,#edf3f8 0%,#e3ebf3 100%); padding:28px 32px; border-bottom:1px solid #cad6e2; }}
  .header img {{ height:48px; }}
  .header-title {{ font-size:22px; font-weight:800; color:#0f1720; margin-top:10px; }}
  .header-title span {{ color:#c99a3c; }}
  .body {{ padding:32px; }}
  .body p {{ font-size:15px; color:#5f7387; line-height:1.7; margin:0 0 16px; }}
  .body strong {{ color:#0f1720; }}
  .features {{ background:#f5f8fb; border:1px solid #dbe4ec; border-radius:12px; padding:18px 20px; margin:20px 0; }}
  .feature {{ display:flex; align-items:flex-start; gap:10px; margin-bottom:10px; font-size:14px; color:#0f1720; }}
  .feature:last-child {{ margin-bottom:0; }}
  .check {{ color:#2f8f53; font-weight:700; }}
  .cta {{ display:block; text-align:center; margin:24px 0 8px; padding:14px 24px; background:#c99a3c; color:#ffffff; text-decoration:none; border-radius:10px; font-size:15px; font-weight:700; }}
  .footer {{ padding:20px 32px; border-top:1px solid #dbe4ec; font-size:12px; color:#5f7387; text-align:center; line-height:1.6; }}
  .footer a {{ color:#5f7387; }}
  .notice {{ margin:20px 0 0; padding:14px 16px; background:#fff8f0; border:1px solid #f0d9b8; border-left:3px solid #c99a3c; border-radius:10px; font-size:13px; color:#5f7387; line-height:1.6; }}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div class="header-title">DocketZero<span>.</span></div>
    <div style="font-size:13px;color:#5f7387;margin-top:4px">Evidence-first research platform</div>
  </div>
  <div class="body">
    <p>Your account is confirmed. Welcome to DocketZero.</p>
    <p>You now have <strong>free access</strong> to the following archives:</p>
    <div class="features">
      <div class="feature"><span class="check">✓</span> Epstein Files — 6,788 documents from DOJ / EFTA releases</div>
      <div class="feature"><span class="check">✓</span> Panama Papers — coming soon</div>
      <div class="feature"><span class="check">✓</span> Entity search, timelines &amp; relationship views</div>
      <div class="feature"><span class="check">✓</span> Saved searches with alert notifications</div>
    </div>
    <a class="cta" href="https://docketzero.com/dashboard.html">Go to My Dashboard →</a>
    <div class="notice">
      <strong style="color:#0f1720">Reminder:</strong> DocketZero is a public research platform. Do not submit personally identifiable information (PII). Your account data is never sold or shared.
    </div>
  </div>
  <div class="footer">
    © 2026 DocketZero &nbsp;·&nbsp; <a href="https://docketzero.com">docketzero.com</a><br>
    Questions? Reply to this email or contact <a href="mailto:support@docketzero.com">support@docketzero.com</a>
  </div>
</div>
</body>
</html>"""

        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, to_email, msg.as_string())

        logger.info(f"Welcome email sent to {to_email}")

    except Exception as e:
        # Non-critical — log but don't fail the registration
        logger.warning(f"Failed to send welcome email to {to_email}: {e}")


# ─── REGISTER ────────────────────────────────────────────────────────
@auth_bp.route("/register", methods=["POST"])
def register():
    data = request.get_json(silent=True) or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")

    if not email or not valid_email(email):
        return jsonify({"error": "Valid email address required."}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters."}), 400

    pw_hash = generate_password_hash(password)

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (email, password_hash, plan, created_at)
            VALUES (%s, %s, 'free', NOW())
            RETURNING id
            """,
            (email, pw_hash),
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.errors.UniqueViolation:
        return jsonify({"error": "An account with that email already exists."}), 409
    except Exception:
        return jsonify({"error": "Registration failed. Please try again."}), 500

    access_token = create_access_token(
        identity=str(user_id), expires_delta=timedelta(hours=24)
    )
    refresh_token = create_refresh_token(
        identity=str(user_id), expires_delta=timedelta(days=30)
    )

    # Send welcome email (non-blocking — failure won't affect registration)
    send_welcome_email(email)

    return jsonify(
        {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "user": {"id": user_id, "email": email, "plan": "free"},
        }
    ), 201


# ─── LOGIN ───────────────────────────────────────────────────────────
@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, email, password_hash, plan FROM users WHERE email = %s",
            (email,),
        )
        row = cur.fetchone()
    except Exception:
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

    access_token = create_access_token(
        identity=str(row[0]), expires_delta=timedelta(hours=24)
    )
    refresh_token = create_refresh_token(
        identity=str(row[0]), expires_delta=timedelta(days=30)
    )

    return jsonify(
        {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "user": {"id": row[0], "email": row[1], "plan": row[3]},
        }
    )


# ─── ME ──────────────────────────────────────────────────────────────
@auth_bp.route("/me", methods=["GET"])
@jwt_required()
def me():
    user_id = get_jwt_identity()
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, email, plan, created_at, last_login,
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

    return jsonify(
        {
            "id": row[0],
            "email": row[1],
            "plan": row[2],
            "created_at": fmt_dt(row[3]),
            "last_login": fmt_dt(row[4]),
            "notification_email": row[5],
            "notification_inapp": row[6],
            "data_retention_days": row[7],
        }
    )


# ─── LOGOUT ──────────────────────────────────────────────────────────
@auth_bp.route("/logout", methods=["POST"])
@jwt_required()
def logout():
    return jsonify({"message": "Logged out successfully."})


# ─── TOKEN REFRESH ───────────────────────────────────────────────────
@auth_bp.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    user_id = get_jwt_identity()
    new_token = create_access_token(
        identity=user_id, expires_delta=timedelta(hours=24)
    )
    return jsonify({"access_token": new_token})


# ─── NOTIFICATION PREFERENCES ────────────────────────────────────────
@auth_bp.route("/notifications/preferences", methods=["PUT"])
@jwt_required()
def update_notification_prefs():
    user_id = get_jwt_identity()
    data = request.get_json(silent=True) or {}

    email_notif = bool(data.get("notification_email", True))
    inapp_notif = bool(data.get("notification_inapp", True))

    try:
        conn = get_db()
        cur = conn.cursor()
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

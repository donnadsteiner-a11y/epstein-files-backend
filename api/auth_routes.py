"""
api/auth_routes.py — DocketZero Authentication Blueprint
"""

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
import os
import re
from datetime import timedelta

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")
CORS(auth_bp, origins=["https://docketzero.com", "http://docketzero.com", "http://localhost"])

# ─── DB ──────────────────────────────────────────────────────────────
def get_db():
    """Return a new psycopg2 connection using the Render internal DATABASE_URL."""
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ─── HELPERS ─────────────────────────────────────────────────────────
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email))

def fmt_dt(dt):
    return dt.isoformat() if dt else None


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
        pass  # Non-critical — login still succeeds

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

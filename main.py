from dotenv import load_dotenv
load_dotenv()

import os
import secrets
import traceback

from fastapi import FastAPI, Request, Query, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from metadata_service import update_symbol_metadata
from authlib.integrations.starlette_client import OAuth
from metadata_service import update_group_mappings
from breadth_engine import calculate_breadth
from init_db import init_db
from user_service import get_user_by_email
from admin.strategy_lab_service import run_strategy_scan
from ingest_candles import run_incremental_ingestion
from database import engine, Base, SessionLocal
from auth_service import *
from ingest_candles import repair_last_days
from telegram_alert import send_telegram_alert
from zoneinfo import ZoneInfo
from ingestion_logs import get_last_successful_ingestion
from datetime import datetime, timezone
from ingest_candles import repair_intraday_days
from fastapi import APIRouter
from fastapi import Header, HTTPException
from ingest_candles import run_intraday_ingestion, run_market_close_ingestion
from models import Signal, Symbol
from sqlalchemy import func




# --------------------------
# APP INIT
# --------------------------

app = FastAPI()
##init_db()


Base.metadata.create_all(bind=engine)
templates = Jinja2Templates(directory="templates")


# --------------------------
# CSRF TOKEN
# --------------------------

def generate_csrf_token(session):
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]


# --------------------------
# ADMIN DEPENDENCY
# --------------------------

def require_admin(request: Request):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        return HTMLResponse("Access Denied", status_code=403)
    return user


# --------------------------
# GOOGLE OAUTH
# --------------------------

oauth = OAuth()

oauth.register(
    name="google",
    client_id=os.getenv("GOOGLE_CLIENT_ID"),
    client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)


# --------------------------
# ROLE PERMISSIONS
# --------------------------

PERMISSIONS = {
    "viewer": ["/dashboard"],
    "trader": ["/dashboard", "/fno"],
    "admin": ["*"]
}


# --------------------------
# AUTH MIDDLEWARE
# --------------------------

""" class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):

        public_routes = [
            "/login",
            "/auth",
            "/logout",
            "/login/google",
            "/internal/run-ingestion",
        ]

        if request.url.path.startswith("/static"):
            return await call_next(request)

        if request.url.path in public_routes:
            return await call_next(request)

        user = request.session.get("user")

        if not user:
            return RedirectResponse("/login")

        role = user.get("role")

        # Admin can access everything
        if role == "admin":
            return await call_next(request)

        allowed_routes = PERMISSIONS.get(role, [])

        if request.url.path not in allowed_routes:
            return HTMLResponse("Permission Denied", status_code=403)
        
        return await call_next(request)
"""

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):

        # ✅ BYPASS WEBHOOK & INTERNAL APIs
        if request.url.path.startswith("/webhook"):
            return await call_next(request)

        if request.url.path.startswith("/internal"):
            return await call_next(request)

        public_routes = [
            "/login",
            "/auth",
            "/logout",
            "/login/google",
        ]

        if request.url.path.startswith("/static"):
            return await call_next(request)

        if request.url.path in public_routes:
            return await call_next(request)

        user = request.session.get("user")

        if not user:
            return RedirectResponse("/login")

        role = user.get("role")

        if role == "admin":
            return await call_next(request)

        allowed_routes = PERMISSIONS.get(role, [])

        if request.url.path not in allowed_routes:
            return HTMLResponse("Permission Denied", status_code=403)
        
        return await call_next(request)


app.add_middleware(AuthMiddleware)

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET"),
    same_site="lax",
    https_only=True
)


# --------------------------
# LOGIN ROUTES
# --------------------------

@app.get("/login")
async def login(request: Request):

    if request.session.get("user"):
        return RedirectResponse("/dashboard")

    base_url = os.getenv("BASE_URL")
    redirect_uri = f"{base_url}/auth" if base_url else request.url_for("auth")

    return await oauth.google.authorize_redirect(
        request,
        redirect_uri,
        prompt="select_account"
    )


@app.get("/login/google")
async def login_google(request: Request):

    base_url = os.getenv("BASE_URL")
    redirect_uri = f"{base_url}/auth" if base_url else request.url_for("auth")

    return await oauth.google.authorize_redirect(request, redirect_uri)


@app.get("/auth")
async def auth(request: Request):

    token = await oauth.google.authorize_access_token(request)

    resp = await oauth.google.get(
        "https://www.googleapis.com/oauth2/v3/userinfo",
        token=token
    )

    user_info = resp.json()
    email = user_info.get("email")

    if not email:
        return HTMLResponse("<h2>Email not found</h2>", status_code=400)

    user = get_user_by_email(email)

    if not user:
        return HTMLResponse("<h2>Access Denied</h2>", status_code=403)

    request.session["user"] = {
        "email": user.email,
        "role": user.role,
        "plan_type": user.plan_type
    }

    return RedirectResponse("/dashboard", status_code=302)



@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")


@app.get("/")
def root():
    return RedirectResponse("/dashboard")


# --------------------------
# DASHBOARD
# --------------------------

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    ema5: str | None = Query(default=None),
    ema20: str | None = Query(default=None),
):

    data = calculate_breadth(ema5_filter=ema5, ema20_filter=ema20)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "data": data,
            "ema5": ema5,
            "ema20": ema20,
            "user": request.session.get("user"),
            "csrf_token": generate_csrf_token(request.session)
        }
    )


# --------------------------
# ADMIN PANEL
# --------------------------

@app.get("/admin", response_class=HTMLResponse)
def admin_panel(request: Request, user=Depends(require_admin)):

    users = get_all_users()

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "users": users,
            "user": user,
            "csrf_token": generate_csrf_token(request.session)
        }
    )


# --------------------------
# ADMIN USER MANAGEMENT
# --------------------------

@app.post("/admin/add-user")
def admin_add_user(
    request: Request,
    email: str = Form(...),
    role: str = Form(...),
    csrf_token: str = Form(...),
    user=Depends(require_admin)
):

    if csrf_token != request.session.get("csrf_token"):
        return HTMLResponse("Invalid CSRF Token", status_code=403)

    add_user(email, role)
    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/update-role")
def admin_update_role(
    request: Request,
    email: str = Form(...),
    role: str = Form(...),
    csrf_token: str = Form(...),
    user=Depends(require_admin)
):

    if csrf_token != request.session.get("csrf_token"):
        return HTMLResponse("Invalid CSRF Token", status_code=403)
    
    if role == "select":
        return HTMLResponse("Please select a valid role", status_code=400)

    if email == user.get("email") and role != "admin":
        return HTMLResponse("You cannot downgrade yourself", status_code=400)

    target = get_user_by_email(email)
    if target and target["role"] == "admin" and role != "admin":
        if count_admins() <= 1:
            return HTMLResponse("Cannot remove last admin", status_code=400)

    update_user_role(email, role)
    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/delete-user")
def admin_delete_user(
    request: Request,
    email: str = Form(...),
    csrf_token: str = Form(...),
    user=Depends(require_admin)
):

    if csrf_token != request.session.get("csrf_token"):
        return HTMLResponse("Invalid CSRF Token", status_code=403)

    if email == user.get("email"):
        return HTMLResponse("Cannot delete yourself", status_code=400)

    target = get_user_by_email(email)
    if target and target["role"] == "admin":
        if count_admins() <= 1:
            return HTMLResponse("Cannot delete last admin", status_code=400)

    delete_user(email)
    return RedirectResponse("/admin", status_code=302)



# --------------------------
# STRATEGY LAB (ADMIN)
# --------------------------

@app.get("/admin/strategy-lab", response_class=HTMLResponse)
def strategy_lab_page(request: Request, user=Depends(require_admin)):

    data = calculate_breadth()
    return templates.TemplateResponse(
        "strategy_lab.html",
        {
            "request": request,
            "data": data,
            "results": None,
            "selected_strategy": None,
            "selected_timeframe": None,
            "selected_lookback": None,
            "include_live": False
        }
    )


@app.post("/admin/strategy-lab", response_class=HTMLResponse)
def strategy_lab_scan(
    request: Request,
    strategy_type: str | None = Form(None),
    timeframe: str | None = Form(None),
    lookback: int = Form(...),
    include_live: str | None = Form(None),
    universe: str = Form("all"),
    signal_window: str = Form("today"),
    ema_filter: str | None = Form(None),
    user=Depends(require_admin)
):
    use_live_candle = include_live == "true"
    data = calculate_breadth()
   
    results = run_strategy_scan(
        strategy_type=strategy_type,
        timeframe=timeframe,
        lookback=int(lookback),
        use_live_candle=use_live_candle,
        universe=universe,
        signal_window=signal_window,
        ema_filter=ema_filter
    )

    print("RETURNED TO UI:", len(results))

    print("SENDING TO TEMPLATE:", len(results))

    return templates.TemplateResponse(
        "strategy_lab.html",
        {
            "request": request,
            "data": data,
            "results": results,

            "selected_strategy": strategy_type,
            "selected_timeframe": timeframe,
            "selected_lookback": lookback,

            "include_live": use_live_candle,

            

            # NEW
            "selected_universe": universe,
            "selected_ema_filter": ema_filter,
            "selected_signal_window": signal_window
        }
    )


# --------------------------
# ADMIN - RUN INGESTION
# --------------------------

@app.post("/admin/run-ingestion")
def run_ingestion(user=Depends(require_admin)):

    run_incremental_ingestion()
    return RedirectResponse("/admin/strategy-lab", status_code=302)

# --------------------------
# ADMIN - REPAIR DATA
# --------------------------

@app.post("/admin/repair-ingestion")
def repair_ingestion(
    days: int = Form(...),
    request: Request = None,
    user=Depends(require_admin)
):

    email = request.session.get("user", {}).get("email")

    # Current IST time
    ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))

    start_msg = f"""
        Admin Repair Triggered

        Days: {days}
        User: {email}

        Time: {ist_now.strftime("%d %b %Y %I:%M %p IST")}
        """

    
    return RedirectResponse("/admin", status_code=302)

# --------------------------
# Update Metadata
# --------------------------

@app.post("/admin/update-metadata")
def run_metadata_update(request: Request):

    user = request.session.get("user")

    if not user or user.get("role") != "admin":
        return HTMLResponse("Access Denied", status_code=403)

    update_symbol_metadata()

    return RedirectResponse("/admin", status_code=302)

@app.post("/admin/update-group-mapping")
def run_group_update(request: Request):

    user = request.session.get("user")

    if not user or user.get("role") != "admin":
        return HTMLResponse("Access Denied", status_code=403)

    update_group_mappings()

    return RedirectResponse("/admin", status_code=302)

# -----------------------------
# Temp 2H repair for last 60 days
# -----------------------------

@app.post("/admin/repair-intraday")
def repair_intraday(
    days: int = 7,
    user=Depends(require_admin)
):
    repair_intraday_days(days)
    return RedirectResponse("/admin", status_code=302)

# --------------------------
# LOCAL RUN
# --------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000))
    )


# ----------------------------
# Ingestion Cron Job in Github
# ----------------------------

INGESTION_SECRET = os.getenv("INGESTION_SECRET")

@app.post("/internal/run-ingestion")
def run_ingestion_job(
    x_token: str = Header(...),
    job_type: str = Query("intraday")   # 👈 NEW
):

    if x_token != INGESTION_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    try:
        send_telegram_alert(f"🚀 Ingestion triggered: {job_type}")

        if job_type == "intraday":
            run_intraday_ingestion()

        elif job_type == "market_close":
            run_market_close_ingestion()

        else:
            raise ValueError("Invalid job_type")

        send_telegram_alert(f"✅ Ingestion completed: {job_type}")

        return {"status": "success", "job": job_type}
    

    except Exception as e:
        error_text = traceback.format_exc()

        print(error_text)

        send_telegram_alert(
            f"❌ Ingestion FAILED ({job_type})\n\n{str(e)}"
        )

        raise





@app.post("/webhook/tradingview")
@app.post("/webhook/tradingview/")
async def tradingview_webhook(request: Request):

    print("🔥 WEBHOOK HIT")

    db = SessionLocal()

    try:
        payload = await request.json()
        print("📦 Payload:", payload)
        # -------------------
        # Extract fields
        # -------------------
        raw_symbol = payload.get("symbol")
        timeframe = payload.get("timeframe")
        signal_type = payload.get("signal")
        c_time = payload.get("candle_time")

        SECRET = os.getenv("WEBHOOK_SECRET")

        # -------------------
        # Secret validation
        # -------------------
        if payload.get("secret") != SECRET:
            print("secret key mismatch")
            return {"status": "unauthorized"}

        if not raw_symbol:
            print("symbol missing")
            return {"status": "error", "message": "symbol missing"}
        """
        # -------------------
        # Normalize symbol
        # -------------------
        symbol = raw_symbol.strip()

        # Remove exchange prefix
        if ":" in symbol:
            symbol = symbol.split(":")[1]

        # Normalize case
        symbol = symbol.upper()

        # Append suffix
        if not symbol.endswith(".NS") and not symbol.startswith("^"):
            symbol = f"{symbol}.NS"

        print("🔁 Normalized symbol:", symbol)

        # -------------------
        # Validate existence
        # -------------------
        exists = db.query(Symbol).filter_by(symbol=symbol).first()

        if not exists:
            print(f"⚠️ Symbol not tracked: {symbol}")
            return {"status": "ignored", "symbol": symbol}
        """

        # -------------------
        # Normalize symbol (ADVANCED)
        # -------------------
        symbol = raw_symbol.strip()

        is_spread = "-" in symbol

        # -------------------
        # Handle spread symbols (e.g., US10Y-TVC:US02Y)
        # -------------------
        if is_spread:
            parts = symbol.split("-")
            clean_parts = []

            for part in parts:
                if ":" in part:
                    part = part.split(":")[1]

                part = part.upper()
                clean_parts.append(part)

            symbol = "-".join(clean_parts)

        else:
            # -------------------
            # Single symbol
            # -------------------
            if ":" in symbol:
                symbol = symbol.split(":")[1]

            symbol = symbol.upper()

        # -------------------
        # Decide suffix (.NS or not)
        # -------------------
        NON_NS_SYMBOLS = {"DXY", "US10Y", "US02Y"}

        if is_spread:
            parts = symbol.split("-")
            symbol = "-".join([
                p if p in NON_NS_SYMBOLS else f"{p}.NS"
                for p in parts
            ])
        else:
            if symbol not in NON_NS_SYMBOLS and not symbol.startswith("^"):
                symbol = f"{symbol}.NS"

        print("🔁 Normalized symbol:", symbol)

        # -------------------
        # Normalize timestamp (CRITICAL FIX)
        # -------------------
        if c_time:
            ts = datetime.fromisoformat(c_time.replace("Z", "+00:00"))
        else:
            ts = datetime.now(timezone.utc)

        # 👉 Convert to NAIVE UTC (DB SAFE)
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)

        # -------------------
        # Duplicate check
        # -------------------
        existing = db.query(Signal).filter_by(
            symbol=symbol,
            timeframe=timeframe,
            signal_type=signal_type,
            timestamp=ts
        ).first()

        if existing:
            print("⚠️ Duplicate signal ignored")
            return {"status": "duplicate"}

        # -------------------
        # Insert signal
        # -------------------
        signal = Signal(
            symbol=symbol,
            timeframe=timeframe,
            signal_type=signal_type,
            timestamp=ts
        )

        db.add(signal)
        db.commit()

        print("✅ Signal stored")

        return {
            "status": "stored",
            "symbol": symbol,
            "timeframe": timeframe,
            "signal": signal_type
        }

    except Exception as e:
        print("❌ ERROR:", str(e))
        return {"status": "error", "message": str(e)}

    finally:
        db.close()
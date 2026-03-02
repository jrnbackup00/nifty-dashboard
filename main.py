from dotenv import load_dotenv
load_dotenv()

import os
import secrets

from fastapi import FastAPI, Request, Query, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from metadata_service import update_symbol_metadata
from authlib.integrations.starlette_client import OAuth

from breadth_engine import calculate_breadth
from auth_db import (
    get_user,
    get_all_users,
    add_user,
    update_user_role,
    delete_user,
    count_admins
)
from init_db import init_db
from user_service import get_user_by_email
from admin.strategy_lab_service import run_strategy_scan
from ingest_candles import run_incremental_ingestion


# --------------------------
# APP INIT
# --------------------------

app = FastAPI()
templates = Jinja2Templates(directory="templates")

init_db()


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

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):

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
        allowed_routes = PERMISSIONS.get(role, [])

        if "*" in allowed_routes:
            return await call_next(request)

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
    import os
    print("DASHBOARD ROUTE PID:", os.getpid())
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

    if email == user.get("email") and role != "admin":
        return HTMLResponse("You cannot downgrade yourself", status_code=400)

    target = get_user(email)
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

    target = get_user(email)
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

    return templates.TemplateResponse(
        "strategy_lab.html",
        {
            "request": request,
            "results": None,
            "selected_strategy": None,
            "selected_timeframe": None,
            "selected_lookback": None
        }
    )


@app.post("/admin/strategy-lab", response_class=HTMLResponse)
def strategy_lab_scan(
    request: Request,
    strategy_type: str = Form(...),
    timeframe: str = Form(...),
    lookback: int = Form(...),
    user=Depends(require_admin)
):

    results = run_strategy_scan(
        strategy_type=strategy_type,
        timeframe=timeframe,
        lookback=int(lookback)
    )

    return templates.TemplateResponse(
        "strategy_lab.html",
        {
            "request": request,
            "results": results,
            "selected_strategy": strategy_type,
            "selected_timeframe": timeframe,
            "selected_lookback": lookback
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
# Update Metadata
# --------------------------

@app.post("/admin/update-metadata")
def run_metadata_update(request: Request):

    user = request.session.get("user")

    if not user or user.get("role") != "admin":
        return HTMLResponse("Access Denied", status_code=403)

    update_symbol_metadata()

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
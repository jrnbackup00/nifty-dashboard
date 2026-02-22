from dotenv import load_dotenv
import os
load_dotenv()
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from authlib.integrations.starlette_client import OAuth
from starlette.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from breadth_engine import calculate_breadth
from auth_db import init_db, get_user, has_permission
from auth_db import get_user, get_all_users
from fastapi import Form
from fastapi.responses import RedirectResponse
from auth_db import add_user
from auth_db import update_user_role
from auth_db import delete_user
from auth_db import delete_user, count_admins

import secrets

def generate_csrf_token(session):
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --------------------------
# INIT DATABASE
# --------------------------
init_db()

# --------------------------
# GOOGLE OAUTH CONFIG
# --------------------------
oauth = OAuth()

oauth.register(
    name="google",
    client_id=os.getenv("GOOGLE_CLIENT_ID"),
    client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={
        "scope": "openid email profile"
    }
)

PERMISSIONS = {
    "viewer": ["/dashboard"],
    "trader": ["/dashboard", "/fno"],
    "admin": ["*"]  # full access
}

# --------------------------
# AUTH MIDDLEWARE (GLOBAL)
# --------------------------
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):

        public_routes = [
            "/login",
            "/auth",
            "/logout",
            "/login/google"
        ]

        # Allow static files
        if request.url.path.startswith("/static"):
            return await call_next(request)

        # Allow public routes
        if request.url.path in public_routes:
            return await call_next(request)

        user = request.session.get("user")

        if not user:
            return RedirectResponse("/login")

        role = user.get("role")
        allowed_routes = PERMISSIONS.get(role, [])

        # Admin has full access
        if "*" in allowed_routes:
            return await call_next(request)

        # Exact route match
        if request.url.path not in allowed_routes:
            return HTMLResponse("Permission Denied", status_code=403)

        return await call_next(request)


# --------------------------
# SESSION MIDDLEWARE
# --------------------------
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

    # If already logged in, go to dashboard
    if request.session.get("user"):
        return RedirectResponse("/dashboard")

    redirect_uri = request.url_for("auth")

    return await oauth.google.authorize_redirect(
        request,
        redirect_uri,
        prompt="select_account"
    )



@app.get("/login/google")
async def login_google(request: Request):
    redirect_uri = request.url_for("auth")
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

    user = get_user(email)   # ← must return dict with role

    if not user:
        return HTMLResponse("<h2>Access Denied</h2>", status_code=403)

    request.session["user"] = {
        "email": user["email"],
        "role": user["role"]
    }

    return RedirectResponse("/dashboard", status_code=302)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)

# --------------------------
# DASHBOARD
# --------------------------
@app.get("/", response_class=HTMLResponse)


@app.get("/admin", response_class=HTMLResponse)
def admin_panel(request: Request):

    user = request.session.get("user")

    if not user or user.get("role") != "admin":
        return HTMLResponse("<h2>Access Denied</h2>", status_code=403)

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
# ADMIN ADD USER
# --------------------------
@app.post("/admin/add-user")
def admin_add_user(
    request: Request,
    email: str = Form(...),
    role: str = Form(...),
    csrf_token: str = Form(...)
):
    user = request.session.get("user")

    if not user or user.get("role") != "admin":
        return HTMLResponse("Access Denied", status_code=403)
    
    if csrf_token != request.session.get("csrf_token"):
        return HTMLResponse("Invalid CSRF Token", status_code=403)

    add_user(email, role)

    return RedirectResponse("/admin", status_code=302)


# --------------------------
# ADMIN UPDATE ROLE
# --------------------------
@app.post("/admin/update-role")
def admin_update_role(
    request: Request,
    email: str = Form(...),
    role: str = Form(...),
    csrf_token: str = Form(...)
):
    user = request.session.get("user")

    if not user or user.get("role") != "admin":
        return HTMLResponse("Access Denied", status_code=403)
    
    if csrf_token != request.session.get("csrf_token"):
        return HTMLResponse("Invalid CSRF Token", status_code=403)


    # Prevent self downgrade
    if email == user.get("email") and role != "admin":
        return HTMLResponse("You cannot downgrade yourself", status_code=400)

    # Prevent removing last admin
    target = get_user(email)
    if target and target["role"] == "admin" and role != "admin":
        if count_admins() <= 1:
            return HTMLResponse("Cannot remove last admin", status_code=400)

    update_user_role(email, role)

    return RedirectResponse("/admin", status_code=302)

# --------------------------
# ADMIN DELETE USER
# --------------------------
@app.post("/admin/delete-user")
def admin_delete_user(
    request: Request,
    email: str = Form(...),
    csrf_token: str = Form(...)
):
    user = request.session.get("user")

    if not user or user.get("role") != "admin":
        return HTMLResponse("Access Denied", status_code=403)
    
    if csrf_token != request.session.get("csrf_token"):
        return HTMLResponse("Invalid CSRF Token", status_code=403)


    # Prevent self delete
    if email == user.get("email"):
        return HTMLResponse("Cannot delete yourself", status_code=400)

    # Prevent deleting last admin
    target = get_user(email)
    if target and target["role"] == "admin":
        if count_admins() <= 1:
            return HTMLResponse("Cannot delete last admin", status_code=400)

    delete_user(email)

    return RedirectResponse("/admin", status_code=302)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000))
    )
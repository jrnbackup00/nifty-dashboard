from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from breadth_engine import calculate_breadth

app = FastAPI()   # 👈 MUST come before any @app.get

templates = Jinja2Templates(directory="templates")


@app.get("/")
def root():
    return {"status": "running"}


@app.get("/breadth")
def breadth():
    return calculate_breadth()


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    data = calculate_breadth()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "data": data
    })

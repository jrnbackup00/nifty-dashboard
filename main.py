from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from breadth_engine import calculate_breadth

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    ema5: str | None = Query(default=None),
    ema20: str | None = Query(default=None),
):
    print("Route received:", ema5, ema20)

    data = calculate_breadth(
        ema5_filter=ema5,
        ema20_filter=ema20
    )

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "data": data,
            "ema5": ema5,
            "ema20": ema20
        }
    )

print("NEW VERSION LOADED 🚀 AUTO UNIVERSE")

from fastapi import FastAPI

print("APP STARTED SUCCESSFULLY 🚀")

app = FastAPI()

@app.get("/")
def home():
    return {"message": "If you see this, deployment works"}

from universe import load_stock_universe
@app.get("/stocks")
def get_stocks():
    stocks = load_stock_universe()
    return {
        "count": len(stocks),
        "stocks": stocks
    }

from breadth_engine import calculate_breadth

@app.get("/breadth")
def breadth():
    return calculate_breadth()

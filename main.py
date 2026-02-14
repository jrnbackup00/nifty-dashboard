from fastapi import FastAPI

print("APP STARTED SUCCESSFULLY 🚀")

app = FastAPI()

@app.get("/")
def home():
    return {"message": "If you see this, deployment works"}

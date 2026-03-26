from fastapi import FastAPI

app = FastAPI(title="notification-service")

@app.get("/health")
def health():
    return {"status": "ok"}
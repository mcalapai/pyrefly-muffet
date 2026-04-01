from fastapi import FastAPI

from admin import router as admin_router
from routers import router

app = FastAPI()
app.include_router(router)
app.include_router(admin_router)


@app.get("/")
def read_root():
    return {"ok": True}

from fastapi import APIRouter

router = APIRouter()


@router.post("/admin")
def create_admin():
    return {"created": True}

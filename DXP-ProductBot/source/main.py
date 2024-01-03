import uvicorn
from api_func import *
from check_regular_product import process_regular_product
from check_sale_product import process_sale_product
from datetime import datetime
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()


def verify_password(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = "admin"
    correct_password = "password"

    if (
        credentials.username != correct_username
        or credentials.password != correct_password
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")


app = FastAPI()


@app.get("/check_regular_product/")
async def check_regular_product(
    date: str | None = None,
    credentials: HTTPBasicCredentials = Security(verify_password),
):
    try:
        current_time = datetime.now()
        yyyymmdd = date if date and len(date) == 8 else current_time.strftime("%Y%m%d")
        result = process_regular_product(yyyymmdd)
        return result
    except HTTPException as e:
        return {"status": e.status_code, "error_message": e.detail}


@app.get("/check_sale_product/")
async def check_sale_product(
    date: str | None = None,
    credentials: HTTPBasicCredentials = Security(verify_password),
):
    try:
        current_time = datetime.now()
        yyyymmdd = date if date and len(date) == 8 else current_time.strftime("%Y%m%d")
        result = process_sale_product(yyyymmdd)
        return result
    except HTTPException as e:
        return {"status": e.status_code, "error_message": e.detail}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

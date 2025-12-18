import os
import json
import pymysql
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from typing import Dict
from pydantic import BaseModel

DB_USER = 'sql12812238'
DB_PASSWORD = 'gTxAhdTlxE'
DB_HOST = 'sql12.freesqldatabase.com'
DB_NAME = 'sql12812238'

TABLE_DETAILS = {
    "CPI": {
        "Observation": "cpi_observations",
        "TwelveMonthPercentageChange": "cpi_twelve_month_percent_change",
        "OneMonthPercentageChange": "cpi_one_month_percent_change"
    },
    "RPI": {
        "Observation": "rpi_observations",
        "TwelveMonthPercentageChange": "rpi_twelve_month_percent_change",
        "OneMonthPercentageChange": "rpi_one_month_percent_change"
    },
    "CPIH": {
        "Observation": "cpih_observations",
        "TwelveMonthPercentageChange": "cpih_twelve_month_percent_change",
        "OneMonthPercentageChange": "cpih_one_month_percent_change"
    }
}

class InflationRequest(BaseModel):
    type: str
    subtype: str
    startyear: int

def get_db():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER,
                           password=DB_PASSWORD, db=DB_NAME, charset='utf8mb4',
                           cursorclass=pymysql.cursors.Cursor)
    try:
        yield conn
    finally:
        conn.close()

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(key: str = Security(api_key_header), conn = Depends(get_db)) -> Dict:
    if not key:
        raise HTTPException(status_code=401, detail="API key missing")

    cur = conn.cursor()
    cur.execute("""
        SELECT u.id, u.username, u.full_name, u.email, u.is_active, ak.id as api_key_id
        FROM api_keys ak
        JOIN users u ON ak.user_id = u.id
        WHERE ak.api_key = %s AND ak.is_active = 1
        LIMIT 1
    """, (key,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=403, detail="Invalid or inactive API key")

    user = {
        "id": row[0],
        "username": row[1],
        "full_name": row[2],
        "email": row[3],
        "is_active": bool(row[4]),
        "api_key_id": row[5]
    }
    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="User is inactive")
    return user

app = FastAPI()

@app.post("/data")
def read_data(body: InflationRequest, user: dict = Depends(verify_api_key), conn = Depends(get_db)):
    table_name = None
    try:
        table_name = TABLE_DETAILS[body.type][body.subtype]
    except KeyError:
        raise HTTPException(status_code=400, detail="Invalid type or subtype")

    cur = conn.cursor()
    columns = ["year"]

    if body.subtype != "OneMonthPercentageChange":
        columns.append("annual_average")

    columns.extend([
        "`Jan`", "`Feb`", "`Mar`", "`Apr`", "`May`", "`Jun`",
        "`Jul`", "`Aug`", "`Sep`", "`Oct`", "`Nov`", "`Dec`"
    ])
    query = f"""SELECT {", ".join(columns)} FROM `{table_name}` WHERE year >= %s ORDER BY year"""
    print(query)
    cur.execute(query, (body.startyear,))
    rows = cur.fetchall()
    clean_columns = [col.strip("`") for col in columns]
    result = [dict(zip(clean_columns, row)) for row in rows]
    return result

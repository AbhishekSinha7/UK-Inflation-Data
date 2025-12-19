import requests
import pandas as pd
from io import BytesIO
from pydantic import BaseModel
import mysql.connector
import pymysql
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from typing import Dict

EXCEL_FILE_ENDPOINT = "https://www.ons.gov.uk/file?uri=/economy/inflationandpriceindices/datasets/consumerpriceinflation/current/consumerpriceinflationdetailedreferencetables.xlsx"
CPI_SHEET_NAME = "Table 15a, 15b, 15c"
CPIH_SHEET_NAME = "Table 6a, 6b, 6c"
RPI_OBSERVATIONS_SHEET_NAME = "Table 23"
RPI_PERCENTAGE12_SHEET_NAME = "Table 24"
RPI_PERCENTAGE1_SHEET_NAME = "Table 25"

DB_USER = 'sql12812238'
DB_PASSWORD = 'gTxAhdTlxE'
DB_HOST = 'sql12.freesqldatabase.com'
DB_NAME = 'sql12812238'

def getExcelFile():
    xls = None
    response = requests.get(EXCEL_FILE_ENDPOINT)

    if response.status_code != 200:
        raise Exception(f"Download failed: {response.status_code}")

    xls = pd.ExcelFile(BytesIO(response.content))
    return xls

app = FastAPI()

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

def getCpiData(xls):
    df_Cpi = pd.read_excel(xls, sheet_name=CPI_SHEET_NAME, header=5)
    
    # Find index of the first row where ALL columns are empty
    records = df_Cpi.to_dict(orient="records")
    observations = {}
    prcntChngTweleveMonths = {}
    prcntChngOneMonths = {}

    for row in records:

        # Year is stored in "Unnamed: 1"
        year_value = row.get("Unnamed: 1")

        # Skip rows where year is missing or non-numeric
        if pd.isna(year_value) or not isinstance(year_value, (int, float)) or not (1000 <= int(year_value) <= 9999):
            continue

        year_key = int(year_value)
        month_values = {}

        for col_name, value in row.items():

            # Skip year + row-label column
            if col_name in ("Unnamed: 0", "Unnamed: 1"):
                continue

            clean_key = col_name.strip().lower()
            
            clean_value = None if pd.isna(value) else value

            month_values[clean_key] = clean_value

        if year_key not in observations:
            observations[year_key] = month_values

        elif year_key not in prcntChngTweleveMonths:
            prcntChngTweleveMonths[year_key] = month_values

        elif year_key not in prcntChngOneMonths:
            # to shift values by 1
            keys = list(month_values.keys())
            values = list(month_values.values())
            shifted_values = [None] + values[:-1]
            prcntChngOneMonths[year_key] = dict(zip(keys, shifted_values))
    
    insertData(observations, 'cpi_observations', True)
    insertData(prcntChngTweleveMonths, 'cpi_twelve_month_percent_change', True)
    insertData(prcntChngOneMonths, 'cpi_one_month_percent_change', False)

def getCpihData(xls):
    df_Cpih = pd.read_excel(xls, sheet_name=CPIH_SHEET_NAME, header=5)
    
    records = df_Cpih.to_dict(orient="records")
    observations = {}
    prcntChngTweleveMonths = {}
    prcntChngOneMonths = {}

    for row in records:
        year_value = row.get("Unnamed: 1")

        # Skip metadata rows such as "2015=100"
        if pd.isna(year_value) or not isinstance(year_value, (int, float)) or not (1000 <= int(year_value) <= 9999):
            continue

        year_key = int(year_value)
       
        month_values = {}
        for col, val in row.items():
            if col in ("Unnamed: 0", "Unnamed: 1"):
                continue  # skip this columns

            clean_col = col.strip().lower()
            clean_val = None if pd.isna(val) else val

            month_values[clean_col] = clean_val

        if year_key not in observations:
            observations[year_key] = month_values

        elif year_key not in prcntChngTweleveMonths:
            prcntChngTweleveMonths[year_key] = month_values

        elif year_key not in prcntChngOneMonths:
            # to shift values by 1
            keys = list(month_values.keys())
            values = list(month_values.values())
            shifted_values = [None] + values[:-1]
            prcntChngOneMonths[year_key] = dict(zip(keys, shifted_values))

    insertData(observations, 'cpih_observations', True)
    insertData(prcntChngTweleveMonths, 'cpih_twelve_month_percent_change', True)
    insertData(prcntChngOneMonths, 'cpih_one_month_percent_change', False)

def getRpiData(xls):
    xls = getExcelFile()
    jsonData = getRpiObservations(xls)
    insertData(jsonData, 'rpi_observations', True)
    jsonData = getRpiPercentage12Months(xls)
    insertData(jsonData, 'rpi_twelve_month_percent_change', True)
    jsonData = getRpiPercentage1Months(xls)
    insertData(jsonData, 'rpi_one_month_percent_change', False)

def getRpiObservations(xls):
    df_Rpih = pd.read_excel(xls, sheet_name=RPI_OBSERVATIONS_SHEET_NAME, header=5)
    json_data = df_Rpih.to_dict(orient="records")

    nested = {}

    for row in json_data:
        # print(row)
        year_key = str(row.get("Unnamed: 2"))
        # print(year_key)
        if not(year_key.isdigit()):
            continue 
        year_key = int(year_key)
        value_map = {}

        for col, val in row.items():
            clean_col = col.strip().lower() 
            if "unnamed:" in clean_col:
                continue

            clean_val = None if pd.isna(val) else val # NaN → null

            if isinstance(clean_val, str):
                clean_val = clean_val.strip()

            value_map[clean_col] = clean_val

        nested[year_key] = value_map
    print(nested)
    return nested

def getRpiPercentage12Months(xls):
    df_Rpih = pd.read_excel(xls, sheet_name=RPI_PERCENTAGE12_SHEET_NAME, header=6)
    json_data = df_Rpih.to_dict(orient="records")
    nested = {}

    for row in json_data:

        year_key = str(row.get("Unnamed: 2"))
        if not(year_key.isdigit()):
            continue
        year_key = int(year_key)

        value_map = {}

        for col, val in row.items():
            clean_col = col.strip().lower()
            if clean_col == "unnamed: 0" or clean_col == "unnamed: 2" or clean_col == "per cent":
                continue

            if clean_col == "change":
                clean_col = "annual_change"
            clean_val = None if pd.isna(val) else val # NaN → null

            if isinstance(clean_val, str):
                clean_val = clean_val.strip()

            value_map[clean_col] = clean_val

        nested[year_key] = value_map
    return nested

def getRpiPercentage1Months(xls):
    df_Rpih = pd.read_excel(xls, sheet_name=RPI_PERCENTAGE1_SHEET_NAME, header=5)
    json_data = df_Rpih.to_dict(orient="records")
    
    nested = {}
    for row in json_data:

        year_value = str(row.get("Unnamed: 2"))
        if not(year_key.isdigit()):
            continue
        year_key = int(year_value)

        value_map = {}

        for col, val in row.items():
            clean_col = col.strip().lower()
            if clean_col == "unnamed: 0" or clean_col == "unnamed: 2" or clean_col == "per cent":
                continue

            clean_val = None if pd.isna(val) else val # NaN → null

            if isinstance(clean_val, str):
                clean_val = clean_val.strip()

            value_map[clean_col] = clean_val

        nested[year_key] = value_map
    return nested

def insertData(observations, table_name, include_annual):
    conn = mysql.connector.connect(
        host="sql12.freesqldatabase.com",
        user="sql12812238",
        password="gTxAhdTlxE",
        database="sql12812238"
    )

    cursor = conn.cursor()

    columns = ["year"]

    if include_annual:
        columns.append("annual_average")

    columns.extend([
        "Jan","Feb","Mar","Apr","May","Jun",
        "Jul","Aug","Sep","Oct","Nov","`Dec`"
    ])

    column_sql = ", ".join(columns)

    placeholders = ", ".join(["%s"] * len(columns))

    update_columns = [col for col in columns if col != "year"]
    update_sql = ", ".join([f"{col}=VALUES({col})" for col in update_columns])

    sql = f"""
        INSERT INTO {table_name} ({column_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_sql};
    """

    rows = []

    for year, values in observations.items():
        row = [year]

        if include_annual:
            row.append(values.get("average"))

        row.extend([
            values.get("jan"),
            values.get("feb"),
            values.get("mar"),
            values.get("apr"),
            values.get("may"),
            values.get("jun"),
            values.get("jul"),
            values.get("aug"),
            values.get("sep"),
            values.get("oct"),
            values.get("nov"),
            values.get("dec"),
        ])

        rows.append(tuple(row))

    cursor.executemany(sql, rows)
    conn.commit()

    print(f"✅ Inserted / Updated {cursor.rowcount} rows")

@app.post("/Refresh")
def get_date(user: dict = Depends(verify_api_key)):
    xls = getExcelFile()

    try:
        getCpiData(xls)
        getCpihData(xls)
        getRpiData(xls)
        response =  {'result': 'Process successful'}

    except:
        response =  {'result': 'Process unsuccessful'}
    
    return response

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

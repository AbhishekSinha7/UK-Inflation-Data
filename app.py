import requests
import pandas as pd
import json
from io import BytesIO
from fastapi import FastAPI
from pydantic import BaseModel

EXCEL_FILE_ENDPOINT = "https://www.ons.gov.uk/file?uri=/economy/inflationandpriceindices/datasets/consumerpriceinflation/current/consumerpriceinflationdetailedreferencetables.xlsx"
CPI_SHEET_NAME = "Table 15a, 15b, 15c"
CPIH_SHEET_NAME = "Table 6a, 6b, 6c"
RPI_SHEET_NAME = "Table 23"

def getExcelFile():
    xls = None
    response = requests.get(EXCEL_FILE_ENDPOINT)

    if response.status_code != 200:
        raise Exception(f"Download failed: {response.status_code}")

    xls = pd.ExcelFile(BytesIO(response.content))
    return xls

class InflationRequest(BaseModel):
    type: str
    year: int

app = FastAPI()

@app.post("/Inflation")
def get_date(req : InflationRequest):
    print(req)
    response = None
    if req.type.lower() == 'cpi':
        response = {'result': getCpiData(req.year)} 
    elif req.type.lower() == 'rpi':
        response = {'result': getRpiData(req.year)} 
    elif req.type.lower() == 'cpih':
        response = {'result': getCpihData(req.year)} 
    
    if response is None:
        response = {'result': 'Data Not Found'}
    
    return response

def getCpiData(year):
    xls = getExcelFile()
    df_Cpi = pd.read_excel(xls, sheet_name=CPI_SHEET_NAME, header=5)
    
    # Find index of the first row where ALL columns are empty
    empty_row_index = df_Cpi[df_Cpi.isna().all(axis=1)].index.min()
    # If an empty row exists, truncate before that row
    if pd.notna(empty_row_index):
        df_Cpi = df_Cpi.loc[:empty_row_index - 1]
    records = df_Cpi.to_dict(orient="records")
    nested_output = {}

    for row in records:

        # Year is stored in "Unnamed: 1"
        year_value = row.get("Unnamed: 1")

        # Skip rows where year is missing or non-numeric
        if pd.isna(year_value) or not isinstance(year_value, (int, float)):
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

        nested_output[year_key] = month_values
    return nested_output[year]


def getCpihData(year):
    xls = getExcelFile()
    df_Cpih = pd.read_excel(xls, sheet_name=CPIH_SHEET_NAME, header=5)
    
    first_empty_row = df_Cpih[df_Cpih.isna().all(axis=1)].index.min()
    print(first_empty_row)
    if pd.notna(first_empty_row):
        print('records'+str(first_empty_row))
        df_Cpih = df_Cpih.loc[: first_empty_row - 1]

    records = df_Cpih.to_dict(orient="records")
    nested_output = {}

    for row in records:
        year_value = row.get("Unnamed: 1")

        # Skip metadata rows such as "2015=100"
        if pd.isna(year_value) or not isinstance(year_value, (int, float)):
            continue

        year_key = int(year_value)
        month_values = {}

        for col, val in row.items():
            if col in ("Unnamed: 0", "Unnamed: 1"):
                continue  # skip this columns

            clean_col = col.strip().lower()
            clean_val = None if pd.isna(val) else val

            month_values[clean_col] = clean_val

        nested_output[year_key] = month_values

    return nested_output[year]


def getRpiData(year):
    xls = getExcelFile()
    df_Rpih = pd.read_excel(xls, sheet_name=RPI_SHEET_NAME, header=None)
    HEADER_ROW = 5

    headers = df_Rpih.iloc[HEADER_ROW].astype(str).str.strip().tolist()
    print(headers)
    # df_Rpih = df_Rpih.iloc[HEADER_ROW:].copy()
    df_Rpih.columns = headers

    # Remove fully empty rows
    df_Rpih = df_Rpih.dropna(how="all").reset_index(drop=True)

    # Remove duplicate header columns (important!)
    # df_Rpih = df_Rpih.loc[:, ~df_Rpih.columns.duplicated()]
    json_data = df_Rpih.to_dict(orient="records")

    nested = {}

    for row in json_data:

        # The year is present in the "nan" column
        year_value = str(row.get("nan"))
        print(year_value)
        year_key = year_value
        if not(year_key.isdigit()):
            continue 
        year_key = int(year_key)
        value_map = {}

        for col, val in row.items():
            if col == "nan":
                continue

            clean_col = col.strip().lower() 
            # print(str(clean_col)+':'+str(val))
            clean_val = None if pd.isna(val) else val # NaN → null

            if isinstance(clean_val, str):
                clean_val = clean_val.strip()

            value_map[clean_col] = clean_val

        nested[year_key] = value_map
        # print(nested)
    return nested[year]
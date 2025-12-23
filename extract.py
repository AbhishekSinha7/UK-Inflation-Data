import requests
import pandas as pd
from io import BytesIO
import mysql.connector
import pymysql

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

def normalize_ons_value(val):
    if pd.isna(val):
        return None

    if isinstance(val, str):
        val = val.strip()
        if val == "-":
            return 0.0
        if val == "..":
            return None

    try:
        return float(val)
    except:
        return None

def is_valid_year(val):
    return (
        not pd.isna(val)
        and isinstance(val, (int, float, str))
        and str(val).isdigit()
        and 1900 <= int(val) <= 2100
    )

def getCpiData(xls):
    df_Cpi = pd.read_excel(xls, sheet_name=CPI_SHEET_NAME, header=5)
    records = df_Cpi.to_dict(orient="records")

    observations = {}
    prcntChngTweleveMonths = {}
    prcntChngOneMonths = {}

    section = -1
    prev_row_invalid = True

    for row in records:
        year_val = row.get("Unnamed: 1")
        valid_year = is_valid_year(year_val)

        if valid_year and prev_row_invalid:
            section += 1

        prev_row_invalid = not valid_year

        if not valid_year:
            continue

        year = int(year_val)
        month_values = {}

        for col, val in row.items():
            if col in ("Unnamed: 0", "Unnamed: 1"):
                continue

            key = col.strip().lower()
            month_values[key] = normalize_ons_value(val)

        if section == 0:
            observations[year] = month_values

        elif section == 1:
            prcntChngTweleveMonths[year] = month_values

        elif section == 2:
            # Shift values by one month
            keys = list(month_values.keys())
            values = list(month_values.values())
            shifted_values = [None] + values[:-1]
            prcntChngOneMonths[year] = dict(zip(keys, shifted_values))

    insertData(observations, 'cpi_observations', True, True)
    insertData(prcntChngTweleveMonths, 'cpi_twelve_month_percent_change', True, False)
    insertData(prcntChngOneMonths, 'cpi_one_month_percent_change', False, False)


def getCpihData(xls):
    df_Cpih = pd.read_excel(xls, sheet_name=CPIH_SHEET_NAME, header=5)
    
    records = df_Cpih.to_dict(orient="records")
    observations = {}
    prcntChngTweleveMonths = {}
    prcntChngOneMonths = {}

    section = -1
    prev_row_invalid = True

    for row in records:
        year_val = row.get("Unnamed: 1")
        valid_year = is_valid_year(year_val)

        if valid_year and prev_row_invalid:
            section += 1

        prev_row_invalid = not valid_year

        if not valid_year:
            continue

        year = int(year_val)
        month_values = {}

        for col, val in row.items():
            if col in ("Unnamed: 0", "Unnamed: 1"):
                continue

            key = col.strip().lower()
            clean = normalize_ons_value(val)
            month_values[key] = clean
        
        if section == 0:
            observations[year] = month_values
        elif section == 1:
            prcntChngTweleveMonths[year] = month_values
        elif section == 2:
            # to shift values by 1
            keys = list(month_values.keys())
            values = list(month_values.values())
            shifted_values = [None] + values[:-1]
            prcntChngOneMonths[year] = dict(zip(keys, shifted_values))

    insertData(observations, 'cpih_observations', True, True)
    insertData(prcntChngTweleveMonths, 'cpih_twelve_month_percent_change', True, False)
    insertData(prcntChngOneMonths, 'cpih_one_month_percent_change', False, False)

def getRpiData(xls):
    xls = getExcelFile()
    jsonData = getRpiObservations(xls)
    insertData(jsonData, 'rpi_observations', True, True)
    jsonData = getRpiPercentage12Months(xls)
    insertData(jsonData, 'rpi_twelve_month_percent_change', True, False)
    jsonData = getRpiPercentage1Months(xls)
    insertData(jsonData, 'rpi_one_month_percent_change', False, False)

def getRpiObservations(xls):
    df_Rpih = pd.read_excel(xls, sheet_name=RPI_OBSERVATIONS_SHEET_NAME, header=5)
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
            if "unnamed:" in clean_col:
                continue

            clean_val = normalize_ons_value(val)
            value_map[clean_col] = clean_val

        nested[year_key] = value_map
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
                clean_col = "average"
            clean_val = normalize_ons_value(val)
            value_map[clean_col] = clean_val
        nested[year_key] = value_map
    return nested

def getRpiPercentage1Months(xls):
    df_Rpih = pd.read_excel(xls, sheet_name=RPI_PERCENTAGE1_SHEET_NAME, header=5)
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

            clean_val = normalize_ons_value(val)
            value_map[clean_col] = clean_val
        nested[year_key] = value_map
    return nested

def insertData(observations, table_name, include_annual, isAnnualAverage):
    conn = mysql.connector.connect(
        host="sql12.freesqldatabase.com",
        user="sql12812238",
        password="gTxAhdTlxE",
        database="sql12812238"
    )

    cursor = conn.cursor()

    columns = ["year"]
    if include_annual:
        if isAnnualAverage:
            columns.append("annual_average")
        else:
            columns.append("annual_change")

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

    print(f"Inserted / Updated {cursor.rowcount} rows")

def get_data():
    xls = getExcelFile()

    try:
        getCpiData(xls)
        getCpihData(xls)
        getRpiData(xls)
    
    except Exception as e:
        print("ERROR:", e)
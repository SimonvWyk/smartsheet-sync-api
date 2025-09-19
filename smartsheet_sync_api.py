from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
import smartsheet
import psycopg2
import httpx
import re

# -------- CONFIG --------
CLIENT_ID = "ttkwuylxtl9ei9zkgsq"
CLIENT_SECRET = "iib4i43syy6bnpds3hn"
REDIRECT_URI = "https://smartsheet-sync.onrender.com/oauth/callback"

SHEET_ID = 7900013460934532
UNIQUE_KEY = "id"

PG_CONFIG = {
    "host": "102.37.33.72",
    "port": 5432,
    "dbname": "postgres",
    "user": "smartsheet",
    "password": "e8kXDw2a497VjgChdJPuny"
}
TABLE_NAME = "draftprojects"

ACCESS_TOKEN = None  # This will be set after user logs in via /install

app = FastAPI(title="Smartsheet Sync App")

# -------- HOMEPAGE --------
@app.get("/")
def home():
    return {
        "message": "✅ Smartsheet Sync App is running",
        "status": "ok",
        "docs": "/docs",
        "install": "/install",
        "sync": "/sync"
    }

# -------- HELPERS --------
def clean_numeric(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("R"):
            s = s[1:]
        s = s.replace(" ", "").replace(",", "")
        if re.match(r'^-?\d+(\.\d+)?$', s):
            return float(s) if '.' in s else int(s)
    return value

def coerce_id(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        s = value.strip().replace(",", "").replace(" ", "")
        m = re.match(r'^-?\d+', s)
        if m:
            return int(m.group(0))
    raise ValueError(f"Could not coerce UNIQUE_KEY '{value}' to int")


# -------- SYNC LOGIC --------
def run_sync(token: str):
    smartsheet_client = smartsheet.Smartsheet(token)
    sheet = smartsheet_client.Sheets.get_sheet(SHEET_ID)

    colid_to_title = {col.id: col.title for col in sheet.columns}
    data_rows = []
    smartsheet_ids = set()

    for row in sheet.rows:
        row_data = {}
        for cell in row.cells:
            col_name = colid_to_title.get(cell.column_id)
            if not col_name:
                continue
            v = cell.value
            if col_name == UNIQUE_KEY:
                try:
                    v = coerce_id(v)
                except Exception:
                    v = None
            else:
                v = clean_numeric(v)
            row_data[col_name] = v

        if row_data.get(UNIQUE_KEY) is not None:
            smartsheet_ids.add(row_data[UNIQUE_KEY])
            data_rows.append(row_data)

    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()

    for row in data_rows:
        keys = list(row.keys())
        values = [row[k] for k in keys]
        updates = ', '.join([f"{k} = EXCLUDED.{k}" for k in keys if k != UNIQUE_KEY])
        sql = f"""
            INSERT INTO {TABLE_NAME} ({', '.join(keys)})
            VALUES ({', '.join(['%s'] * len(values))})
            ON CONFLICT ({UNIQUE_KEY})
            DO UPDATE SET {updates}
        """
        cursor.execute(sql, values)

    cursor.execute(f"SELECT {UNIQUE_KEY} FROM {TABLE_NAME}")
    db_ids = {r[0] for r in cursor.fetchall()}
    missing_ids = db_ids - smartsheet_ids

    deleted_count = 0
    if missing_ids:
        cursor.execute(
            f"DELETE FROM {TABLE_NAME} WHERE {UNIQUE_KEY} = ANY(%s)",
            (list(missing_ids),)
        )
        deleted_count = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    return {"upserted": len(data_rows), "deleted": deleted_count}


# -------- ROUTES --------
@app.get("/install")
def install():
    url = (
        "https://app.smartsheet.com/b/authorize"
        f"?response_type=code&client_id={CLIENT_ID}"
        "&scope=READ_SHEETS WRITE_SHEETS"
        f"&redirect_uri={REDIRECT_URI}"
    )
    return RedirectResponse(url)

@app.get("/oauth/callback")
def oauth_callback(code: str):
    global ACCESS_TOKEN
    token_url = "https://api.smartsheet.com/2.0/token"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI
    }
    auth = (CLIENT_ID, CLIENT_SECRET)
    resp = httpx.post(token_url, data=data, auth=auth)
    data = resp.json()
    ACCESS_TOKEN = data.get("access_token")
    if not ACCESS_TOKEN:
        return {"error": "Failed to get access token", "response": data}
    return {"message": "Authorized successfully", "access_token": ACCESS_TOKEN}

@app.post("/sync")
def sync_now():
    if not ACCESS_TOKEN:
        raise HTTPException(status_code=401, detail="Not authorized. Visit /install first.")
    try:
        result = run_sync(ACCESS_TOKEN)
        return {"status": "success", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

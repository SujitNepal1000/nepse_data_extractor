import os, json, sys, traceback
from google.oauth2.service_account import Credentials
import gspread

sa_json = os.environ.get('GSP_SA_KEY')
sheet_id = os.environ.get('GSHEET_ID')

def fail(msg):
    print("FAIL:", msg)
    sys.exit(1)

if not sa_json:
    fail("GSP_SA_KEY is empty or not set")

if not sheet_id:
    fail("GSHEET_ID is empty or not set")

try:
    creds_dict = json.loads(sa_json)
except Exception as e:
    print("Failed to parse service-account JSON:", e)
    traceback.print_exc()
    sys.exit(2)

try:
    scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
except Exception as e:
    print("Auth failed:", e)
    traceback.print_exc()
    sys.exit(3)

try:
    sh = client.open_by_key(sheet_id)
    print("SUCCESS: Opened spreadsheet:", sh.title)
    ws_list = [ws.title for ws in sh.worksheets()]
    print("Worksheets:", ws_list)
    print("Service account email:", creds_dict.get('client_email'))
except Exception as e:
    print("Failed to open sheet:", e)
    traceback.print_exc()
    sys.exit(4)

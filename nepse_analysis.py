import os
import time
import requests
import pandas as pd
import numpy as np
import logging
import json
from urllib.parse import unquote
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe

SHARESANSAR_AJAX = 'https://www.sharesansar.com/ajaxtodayshareprice'
SHARESANSAR_URL = 'https://www.sharesansar.com/today-share-price'
START_DATE = datetime(2025, 7, 1)
END_DATE = datetime.now()
OUTPUT_FILE = 'output/nepse_analysis.xlsx'
LOG_FILE = 'logs/nepse_analysis.log'
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
EMA_TREND = 50

def setup_logging():
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def _clean_token(t):
    if not t:
        return ''
    t = str(t)
    t = unquote(t)
    if t.startswith('"') and t.endswith('"'):
        t = t[1:-1]
    return t.strip()

def post_with_csrf(session, ajax_url, page_url, date_str, max_retries=3, base_delay=1.0):
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(page_url, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            token = None
            t_input = soup.find('input', {'name': '_token'})
            if t_input and t_input.get('value'):
                token = t_input['value']
            else:
                meta = soup.find('meta', {'name': 'csrf-token'})
                if meta and meta.get('content'):
                    token = meta['content']
            if not token:
                cookie_val = session.cookies.get('XSRF-TOKEN') or session.cookies.get('XSRF_TOKEN')
                token = cookie_val
            token = _clean_token(token)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
                'Referer': page_url,
                'Origin': 'https://www.sharesansar.com',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'X-Requested-With': 'XMLHttpRequest',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
            }
            if token:
                headers['X-XSRF-TOKEN'] = token
                headers['X-CSRF-TOKEN'] = token
            payload = {'_token': token or '', 'sector': 'all_sec', 'date': date_str}
            resp = session.post(ajax_url, data=payload, headers=headers, timeout=20)
            if resp.status_code == 419:
                raise requests.exceptions.HTTPError('419')
            resp.raise_for_status()
            text = resp.text
            try:
                return resp.json()
            except ValueError:
                snippet = text[:2000].replace('\n', ' ')
                print(f'[attempt {attempt}] non-json response len={len(text)} snippet={snippet[:400]}')
                try:
                    soup = BeautifulSoup(text, 'html.parser')
                    table = soup.find('table')
                    if table:
                        thead = table.find('thead')
                        if thead:
                            headers = [th.get_text(strip=True) for th in thead.find_all('th')]
                            tbody = table.find('tbody')
                        else:
                            headers = [th.get_text(strip=True) for th in table.find_all('th')]
                            tbody = table
                        rows = []
                        for tr in tbody.find_all('tr'):
                            cols = [td.get_text(strip=True) for td in tr.find_all('td')]
                            if cols and len(cols) == len(headers):
                                rows.append(dict(zip(headers, cols)))
                        if rows:
                            return {'data': rows}
                except Exception:
                    pass
                return None
        except requests.exceptions.HTTPError as he:
            print(f'[attempt {attempt}] http error for {date_str}: {he}')
        except Exception as e:
            print(f'[attempt {attempt}] error for {date_str}: {e}')
        sleep_time = base_delay * (2 ** (attempt - 1))
        time.sleep(sleep_time)
    raise RuntimeError(f'failed to fetch data for {date_str} after {max_retries} attempts')

def fetch_with_selenium(date_str, timeout=30):
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        driver.get(SHARESANSAR_URL)
        time.sleep(2)
        script = (
            "const date = arguments[0];"
            "const callback = arguments[arguments.length-1];"
            "try{"
            "  const tokenEl = document.querySelector('input[name=\"_token\"]');"
            "  const token = tokenEl ? tokenEl.value : (document.querySelector('meta[name=\"csrf-token\"]') ? document.querySelector('meta[name=\"csrf-token\"]').content : '');"
            "  const body = new URLSearchParams();"
            "  body.append('_token', token);"
            "  body.append('sector','all_sec');"
            "  body.append('date', date);"
            "  fetch('/ajaxtodayshareprice', {method:'POST', body: body, headers:{'X-Requested-With':'XMLHttpRequest'}})"
            "    .then(r=>r.text()).then(t=>callback(t)).catch(e=>callback(JSON.stringify({error:String(e)})));"
            "}catch(e){callback(JSON.stringify({error:String(e)}));}"
        )
        result_text = driver.execute_async_script(script, date_str)
        if not result_text:
            return None
        try:
            parsed = json.loads(result_text)
            return parsed
        except Exception:
            idx = result_text.find('{')
            if idx >= 0:
                try:
                    parsed = json.loads(result_text[idx:])
                    return parsed
                except Exception:
                    pass
        return None
    finally:
        driver.quit()

def _rows_from_table_html(html_text):
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        table = soup.find('table')
        if not table:
            return []
        thead = table.find('thead')
        if thead:
            headers = [th.get_text(strip=True) for th in thead.find_all('th')]
            tbody = table.find('tbody') or table
        else:
            headers = [th.get_text(strip=True) for th in table.find_all('th')]
            tbody = table
        rows = []
        for tr in tbody.find_all('tr'):
            cols = [td.get_text(strip=True) for td in tr.find_all('td')]
            if cols and len(cols) == len(headers):
                rows.append(dict(zip(headers, cols)))
        return rows
    except Exception:
        return []

def fetch_historical_data(start_date, end_date):
    all_rows = []
    session_latest = requests.Session()
    session_latest.headers.update({'User-Agent': 'Mozilla/5.0'})
    try:
        latest_resp = post_with_csrf(session_latest, SHARESANSAR_AJAX, SHARESANSAR_URL, END_DATE.strftime('%Y-%m-%d'))
    except Exception:
        latest_resp = None
    latest_symbols = set()
    latest_snapshot_signature = None
    if latest_resp:
        latest_data = latest_resp.get('data') if isinstance(latest_resp, dict) and 'data' in latest_resp else latest_resp
        try:
            df_latest = pd.DataFrame(latest_data)
            if not df_latest.empty and 'symbol' in df_latest.columns:
                latest_symbols = set(df_latest['symbol'].astype(str))
                numeric_cols = ['close', 'prev_close', 'volume']
                sig_parts = []
                for c in numeric_cols:
                    if c in df_latest.columns:
                        vals = df_latest[c].astype(str).fillna('').tolist()[:5]
                        sig_parts.append('|'.join(vals))
                latest_snapshot_signature = '||'.join(sig_parts)
        except Exception:
            latest_symbols = set()
    current = start_date
    while current.date() <= end_date.date():
        date_str = current.strftime('%Y-%m-%d')
        print(f'Fetching {date_str} ...')
        try:
            session = requests.Session()
            session.headers.update({'User-Agent': 'Mozilla/5.0'})
            result = post_with_csrf(session, SHARESANSAR_AJAX, SHARESANSAR_URL, date_str)
            data = None
            if result is None:
                page = session.get(SHARESANSAR_URL, timeout=15)
                page.raise_for_status()
                rows = _rows_from_table_html(page.text)
                if rows:
                    df_day = pd.DataFrame(rows)
                    df_day.columns = [str(c).strip() for c in df_day.columns]
                    df_day['date'] = pd.to_datetime(date_str).date()
                    all_rows.append(df_day)
                    print(f'fallback html: {len(df_day)} rows for {date_str}')
                    logging.info(f'fallback html fetched {len(df_day)} rows for {date_str}')
                    current += timedelta(days=1)
                    time.sleep(0.5)
                    continue
            else:
                data = result.get('data') if isinstance(result, dict) and 'data' in result else result
            if data is not None:
                df_day = pd.DataFrame(data)
                if df_day.empty:
                    print(f'ajax returned empty for {date_str}')
                else:
                    df_day.columns = [str(c).strip() for c in df_day.columns]
                    df_day['date'] = pd.to_datetime(date_str).date()
                    symbols = set(df_day['symbol'].astype(str)) if 'symbol' in df_day.columns else set()
                    sig_parts = []
                    for c in ['close', 'prev_close', 'volume']:
                        if c in df_day.columns:
                            vals = df_day[c].astype(str).fillna('').tolist()[:5]
                            sig_parts.append('|'.join(vals))
                    signature = '||'.join(sig_parts) if sig_parts else None
                    if latest_snapshot_signature and signature == latest_snapshot_signature and date_str != END_DATE.strftime('%Y-%m-%d'):
                        try:
                            selenium_data = fetch_with_selenium(date_str)
                        except Exception:
                            selenium_data = None
                        if selenium_data:
                            data_s = selenium_data.get('data') if isinstance(selenium_data, dict) and 'data' in selenium_data else selenium_data
                            df_s = pd.DataFrame(data_s)
                            if not df_s.empty:
                                df_s.columns = [str(c).strip() for c in df_s.columns]
                                df_s['date'] = pd.to_datetime(date_str).date()
                                all_rows.append(df_s)
                                print(f'selenium rows: {len(df_s)} for {date_str}')
                                logging.info(f'selenium fetched {len(df_s)} rows for {date_str}')
                                current += timedelta(days=1)
                                time.sleep(0.5)
                                continue
                    all_rows.append(df_day)
                    print(f'ajax rows: {len(df_day)} for {date_str}')
                    logging.info(f'fetched {len(df_day)} rows for {date_str}')
            else:
                print(f'no data for {date_str}')
        except Exception as e:
            print(f'final failure for {date_str}: {e}')
            logging.warning(f'failed for {date_str}: {e}')
        time.sleep(0.5)
        current += timedelta(days=1)
    if not all_rows:
        print('no historical data fetched')
        logging.error('no historical data fetched')
        return pd.DataFrame()
    try:
        df_all = pd.concat(all_rows, ignore_index=True, sort=False)
    except Exception:
        df_all = pd.DataFrame()
    if 'date' in df_all.columns:
        df_all['date'] = pd.to_datetime(df_all['date']).dt.date
    return df_all

def compute_rsi(series, period=RSI_PERIOD):
    if series is None or len(series) == 0:
        return pd.Series(np.nan, index=series.index if hasattr(series, 'index') else [])
    s = series.astype(float)
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rs = rs.replace([np.inf, -np.inf], np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi.loc[avg_loss == 0] = 100
    return rsi

def compute_macd(series):
    if series is None or len(series) == 0:
        return pd.Series(np.nan, index=series.index if hasattr(series, 'index') else []), pd.Series(np.nan, index=series.index if hasattr(series, 'index') else [])
    s = series.astype(float)
    ema_fast = s.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = s.ewm(span=MACD_SLOW, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
    return macd, signal

def compute_trend_flags(df):
    if df.empty:
        return df
    df = df.sort_values(['symbol', 'date'])
    for label, days in [('trend_1d', 1), ('trend_1w', 7), ('trend_2w', 14), ('trend_1m', 30)]:
        df[label] = df.groupby('symbol')['close'].transform(lambda x: np.where(x > x.shift(days), 'Up', 'Down'))
    return df

def _find_column(df, candidates):
    cols = {str(c).lower(): c for c in df.columns}
    for c in candidates:
        key = c.lower()
        if key in cols:
            return cols[key]
    return None

def process_data(df):
    if df.empty:
        print('empty dataframe passed to process_data')
        logging.error('empty dataframe passed to process_data')
        return df
    df.columns = [str(c).strip() for c in df.columns]
    mapping_candidates = {
        'symbol': ['symbol', 'scrip', 'code'],
        'open': ['open', 'o'],
        'high': ['high', 'h'],
        'low': ['low', 'l'],
        'close': ['close', 'c', 'closing_price', 'ltpclose', 'ltp'],
        'ltp': ['ltp', 'last', 'last_traded_price'],
        'vwap': ['vwap'],
        'vol': ['vol', 'volume', 'volumne', 'volumn'],
        'prev_close': ['prev_close', 'prev. close', 'previous_close', 'prevclose', 'prev'],
        'turnover': ['turnover', 'turn_over'],
        'trans': ['trans', 'transaction', 'transactions', 'trans.'],
        'diff': ['diff'],
        'range': ['range'],
        '52_high': ['52_weeks_high', '52_week_high', '52_weeks_high', '52_high'],
        '52_low': ['52_weeks_low', '52_week_low', '52_weeks_low', '52_low'],
        'conf': ['conf', 'conf.']
    }
    std_cols = {}
    for std, candidates in mapping_candidates.items():
        std_cols[std] = _find_column(df, candidates)
    df_clean = df.copy()
    for key in ['open', 'high', 'low', 'close', 'ltp', 'vwap', 'vol', 'prev_close', 'turnover', 'diff', 'range', '52_high', '52_low']:
        col = std_cols.get(key)
        if col:
            df_clean[col] = pd.to_numeric(df_clean[col].astype(str).str.replace(',', '', regex=False).str.replace('--', '', regex=False), errors='coerce')
        else:
            df_clean[key] = np.nan
    df_work = pd.DataFrame()
    df_work['symbol'] = df_clean[std_cols.get('symbol')] if std_cols.get('symbol') else df_clean.get('symbol', np.nan)
    df_work['conf'] = df_clean[std_cols.get('conf')] if std_cols.get('conf') else df_clean.get('conf', np.nan)
    df_work['open'] = df_clean[std_cols.get('open')] if std_cols.get('open') else df_clean['open']
    df_work['high'] = df_clean[std_cols.get('high')] if std_cols.get('high') else df_clean['high']
    df_work['low'] = df_clean[std_cols.get('low')] if std_cols.get('low') else df_clean['low']
    df_work['close'] = df_clean[std_cols.get('close')] if std_cols.get('close') else df_clean['close']
    df_work['ltp'] = df_clean[std_cols.get('ltp')] if std_cols.get('ltp') else df_clean['ltp']
    df_work['vwap'] = df_clean[std_cols.get('vwap')] if std_cols.get('vwap') else df_clean['vwap']
    df_work['vol'] = df_clean[std_cols.get('vol')] if std_cols.get('vol') else df_clean['vol']
    df_work['prev_close'] = df_clean[std_cols.get('prev_close')] if std_cols.get('prev_close') else df_clean['prev_close']
    df_work['turnover'] = df_clean[std_cols.get('turnover')] if std_cols.get('turnover') else df_clean['turnover']
    df_work['trans'] = df_clean[std_cols.get('trans')] if std_cols.get('trans') else df_clean.get('trans', np.nan)
    df_work['52_high'] = df_clean[std_cols.get('52_high')] if std_cols.get('52_high') else df_clean['52_high']
    df_work['52_low'] = df_clean[std_cols.get('52_low')] if std_cols.get('52_low') else df_clean['52_low']
    if std_cols.get('diff'):
        df_work['diff'] = df_clean[std_cols.get('diff')]
    else:
        df_work['diff'] = df_work['close'] - df_work['prev_close']
    if std_cols.get('range'):
        df_work['range'] = df_clean[std_cols.get('range')]
    else:
        df_work['range'] = df_work['high'] - df_work['low']
    df_work['close_minus_ltp'] = df_work['close'] - df_work['ltp']
    df_work['close_minus_ltp_pct'] = np.where(df_work['ltp'] > 0, df_work['close_minus_ltp'] / df_work['ltp'] * 100, np.nan)
    df_work['diff_pct'] = np.where(df_work['prev_close'] > 0, df_work['diff'] / df_work['prev_close'] * 100, np.nan)
    df_work['range_pct'] = np.where(df_work['high'] > 0, df_work['range'] / df_work['high'] * 100, np.nan)
    df_work['vwap_pct'] = np.where(df_work['vwap'] > 0, (df_work['close'] - df_work['vwap']) / df_work['vwap'] * 100, np.nan)
    if 'date' in df_clean.columns:
        df_work['date'] = pd.to_datetime(df_clean['date']).dt.date
    else:
        df_work['date'] = pd.NaT
    df_work = df_work.sort_values(['symbol', 'date']).reset_index(drop=True)
    df_work['rsi'] = df_work.groupby('symbol')['close'].transform(lambda x: compute_rsi(x, period=RSI_PERIOD))
    df_work['macd'] = np.nan
    df_work['signal'] = np.nan
    for name, grp in df_work.groupby('symbol'):
        macd_series, signal_series = compute_macd(grp['close'])
        df_work.loc[grp.index, 'macd'] = macd_series.values
        df_work.loc[grp.index, 'signal'] = signal_series.values
    window = 252
    df_work['52_high_calc'] = df_work.groupby('symbol')['high'].transform(lambda x: x.rolling(window=window, min_periods=1).max())
    df_work['52_low_calc'] = df_work.groupby('symbol')['low'].transform(lambda x: x.rolling(window=window, min_periods=1).min())
    df_work['52_high'] = np.where(df_work['52_high'].notna(), df_work['52_high'], df_work['52_high_calc'])
    df_work['52_low'] = np.where(df_work['52_low'].notna(), df_work['52_low'], df_work['52_low_calc'])
    df_work.drop(columns=['52_high_calc', '52_low_calc'], inplace=True, errors='ignore')
    df_work['vol'] = pd.to_numeric(df_work['vol'], errors='coerce').fillna(0)
    df_work['ema_trend'] = df_work.groupby('symbol')['close'].transform(lambda x: np.where(x > x.ewm(span=EMA_TREND, adjust=False).mean(), 'Uptrend', 'Downtrend'))
    df_work = compute_trend_flags(df_work)
    output_cols = ['symbol', 'conf', 'open', 'high', 'low', 'close', 'ltp', 'close_minus_ltp', 'close_minus_ltp_pct', 'vwap', 'vol', 'prev_close', 'turnover', 'trans', 'diff', 'range', 'diff_pct', 'range_pct', 'vwap_pct', '52_high', '52_low', 'rsi', 'macd', 'signal', 'ema_trend', 'trend_1d', 'trend_1w', 'trend_2w', 'trend_1m', 'date']
    for c in output_cols:
        if c not in df_work.columns:
            df_work[c] = np.nan
    return df_work

def upload_to_gsheet(df, spreadsheet_id, service_account_json_str, sheet_prefix='NEPSE'):
    creds_dict = json.loads(service_account_json_str)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    try:
        try:
            ws = sh.worksheet('Master')
            sh.del_worksheet(ws)
        except Exception:
            pass
        ws = sh.add_worksheet(title='Master', rows=str(len(df) + 10), cols='50')
        set_with_dataframe(ws, df)
        for date, group in sorted(df.groupby('date')):
            title = f"{sheet_prefix}-{date}"
            if len(title) > 100:
                title = title[:100]
            try:
                ws = sh.worksheet(title)
                sh.del_worksheet(ws)
            except Exception:
                pass
            ws = sh.add_worksheet(title=title, rows=str(len(group) + 5), cols='50')
            set_with_dataframe(ws, group.reset_index(drop=True))
        return True
    except Exception as e:
        print(f'upload failed: {e}')
        return False

def save_to_excel(df, filename=OUTPUT_FILE):
    if df.empty:
        print('empty dataframe, nothing to save')
        logging.error('empty dataframe, nothing to save')
        return
    import os
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    display_map = [
        ('S.No', None), ('Symbol', 'symbol'), ('Conf.', 'conf'), ('Open', 'open'), ('High', 'high'), ('Low', 'low'), ('Close', 'close'), ('LTP', 'ltp'), ('Close - LTP', 'close_minus_ltp'), ('Close - LTP %', 'close_minus_ltp_pct'), ('VWAP', 'vwap'), ('Vol', 'vol'), ('Prev. Close', 'prev_close'), ('Turnover', 'turnover'), ('Trans.', 'trans'), ('Diff', 'diff'), ('Range', 'range'), ('Diff %', 'diff_pct'), ('Range %', 'range_pct'), ('VWAP %', 'vwap_pct'), ('52 Weeks High', '52_high'), ('52 Weeks Low', '52_low'), ('RSI', 'rsi'), ('MACD', 'macd'), ('Signal', 'signal')
    ]
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for date, group in sorted(df.groupby('date')):
            sheet = str(date)
            if len(sheet) > 31:
                sheet = sheet[:31]
            group = group.reset_index(drop=True).copy()
            group.insert(0, 'S.No', group.index + 1)
            out_cols = []
            out_names = []
            for display_name, col_key in display_map:
                out_names.append(display_name)
                if col_key is None:
                    out_cols.append('S.No')
                else:
                    out_cols.append(col_key)
            for col in out_cols:
                if col not in group.columns:
                    group[col] = np.nan
            out = group[out_cols]
            out.columns = out_names
            out.to_excel(writer, sheet_name=sheet, index=False)
        print(f'saved excel with {len(df.date.unique())} sheets to {filename}')
        logging.info(f'saved excel with {len(df.date.unique())} sheets to {filename}')

def main():
    setup_logging()
    print('starting historical job')
    logging.info('starting historical job')
    start = os.environ.get('START_DATE')
    end = os.environ.get('END_DATE')
    if start:
        try:
            s = datetime.fromisoformat(start)
        except Exception:
            s = START_DATE
    else:
        s = START_DATE
    if end:
        try:
            e = datetime.fromisoformat(end)
        except Exception:
            e = END_DATE
    else:
        e = END_DATE
    df_raw = fetch_historical_data(s, e)
    df = process_data(df_raw)
    save_to_excel(df)
    sa_json = os.environ.get('GSP_SA_KEY')
    sheet_id = os.environ.get('GSHEET_ID')
    if sa_json and sheet_id:
        ok = upload_to_gsheet(df, sheet_id, sa_json, sheet_prefix=os.environ.get('GSHEET_PREFIX', 'NEPSE'))
        print('upload to gsheet:', ok)
    print('job complete')
    logging.info('job complete')

if __name__ == '__main__':
    main()

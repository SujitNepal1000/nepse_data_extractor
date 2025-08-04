import requests
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nepse_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# API configuration
BASE_URL = "https://nepseapi.surajrimal.dev"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds between retries
REQUEST_DELAY = 1  # seconds between API calls

def get_api_data(endpoint, params=None, retries=MAX_RETRIES):
    """Fetch data from API with retry mechanism and status code validation"""
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=300)
            logger.info(f"Request to {url} - Status: {response.status_code}")
            
            # Validate status code
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 524:
                logger.warning(f"Timeout error (524) on attempt {attempt}")
            else:
                logger.warning(f"Unexpected status {response.status_code} on attempt {attempt}")
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed on attempt {attempt}: {str(e)}")
        
        # Wait before retrying
        if attempt < retries:
            logger.info(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    
    logger.error(f"All attempts failed for endpoint: {endpoint}")
    return None

def calculate_rsi(closes, window=14):
    """Calculate Relative Strength Index (RSI)"""
    if len(closes) < window + 1:
        return np.nan
        
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    # Calculate initial averages
    avg_gain = np.mean(gains[:window])
    avg_loss = np.mean(losses[:window])
    
    # Handle edge cases
    if avg_loss == 0:
        return 100.0 if avg_gain != 0 else 50.0
    
    # Calculate RS and RSI
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def get_all_symbols():
    """Fetch all stock symbols from SectorScrips endpoint"""
    logger.info("Fetching all symbols from SectorScrips API")
    sectors_data = get_api_data("/SectorScrips")
    
    if not sectors_data:
        logger.error("Failed to get sector scrips data")
        return []
    
    symbols = []
    for sector, symbol_list in sectors_data.items():
        # Extract symbols from the list
        symbols.extend(symbol_list)
    
    logger.info(f"Found {len(symbols)} symbols across {len(sectors_data)} sectors")
    return symbols

def main():
    # Create output directory
    os.makedirs('NEPSE_Reports', exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = f"NEPSE_Reports/nepse_rsi_report_{today}.xlsx"
    
    logger.info("Starting NEPSE RSI Analysis")
    
    # Step 1: Get all symbols using SectorScrips API
    logger.info("Step 1/6: Fetching all stock symbols")
    all_symbols = get_all_symbols()
    if not all_symbols:
        logger.critical("Could not retrieve symbol list. Exiting.")
        return
    
    # Step 2: Get today's market data
    logger.info("Step 2/6: Fetching today's market data")
    today_data = get_api_data("/PriceVolume")
    if not today_data:
        logger.error("Failed to get today's market data. Exiting.")
        return
    
    # Create mapping for quick lookup
    today_data_map = {item['symbol']: item for item in today_data}
    
    # Prepare results storage
    all_stocks = []
    low_rsi_stocks = []
    failed_symbols = []
    
    # Step 3-5: Process each symbol
    logger.info(f"Step 3-5/6: Processing {len(all_symbols)} symbols")
    for i, symbol in enumerate(all_symbols):
        # Log progress
        if (i + 1) % 10 == 0 or (i + 1) == len(all_symbols):
            logger.info(f"Processing symbol {i+1}/{len(all_symbols)}: {symbol}")
        
        # Step 3: Get historical data
        logger.debug(f"Getting historical data for {symbol}")
        hist_data = get_api_data(f"/HistoricalData", params={"symbol": symbol})
        
        # Validate historical data
        if not hist_data or not hist_data.get('data') or len(hist_data['data']) < 15:
            logger.warning(f"Insufficient historical data for {symbol}")
            failed_symbols.append(symbol)
            continue
        
        # Extract closing prices (last 30 days)
        closes = []
        for day in hist_data['data'][-30:]:
            if 'close' in day and isinstance(day['close'], (int, float)):
                closes.append(day['close'])
        
        if len(closes) < 15:
            logger.warning(f"Only {len(closes)} valid closing prices for {symbol}")
            failed_symbols.append(symbol)
            continue
        
        # Step 5: Calculate RSI
        rsi = calculate_rsi(closes)
        
        # Get today's data for this symbol
        current_data = today_data_map.get(symbol, {})
        
        # Prepare stock record
        stock_record = {
            'Symbol': symbol,
            'Company': current_data.get('securityName', 'N/A'),
            'Sector': current_data.get('sector', 'N/A'),
            'Previous Close': current_data.get('previousClose', np.nan),
            "Today's Close": current_data.get('lastTradedPrice', np.nan),
            'Change (%)': current_data.get('percentageChange', np.nan),
            'Volume': current_data.get('totalTradeQuantity', 0),
            'RSI (14-day)': rsi,
            'Data Points': len(closes)
        }
        
        # Add to results
        all_stocks.append(stock_record)
        
        # Add to low RSI list if applicable
        if rsi < 50:
            low_rsi_stocks.append(stock_record)
        
        # Add delay between requests
        time.sleep(REQUEST_DELAY)
    
    # Step 6: Create Excel report
    logger.info("Step 6/6: Generating Excel report")
    try:
        # Create DataFrames
        all_stocks_df = pd.DataFrame(all_stocks)
        low_rsi_df = pd.DataFrame(low_rsi_stocks)
        
        # Sort low RSI stocks
        if not low_rsi_df.empty:
            low_rsi_df.sort_values('RSI (14-day)', inplace=True)
        
        # Create Excel file with two sheets
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            all_stocks_df.to_excel(writer, sheet_name='All Stocks', index=False)
            
            if not low_rsi_df.empty:
                low_rsi_df.to_excel(writer, sheet_name='RSI Below 50', index=False)
            
            # Auto-adjust column widths
            for sheet_name in writer.sheets:
                sheet = writer.sheets[sheet_name]
                for column in sheet.columns:
                    max_length = 0
                    for cell in column:
                        try:
                            cell_value = str(cell.value) if cell.value is not None else ""
                            if len(cell_value) > max_length:
                                max_length = len(cell_value)
                        except:
                            pass
                    adjusted_width = max_length + 2
                    sheet.column_dimensions[column[0].column_letter].width = adjusted_width
        
        logger.info(f"Report generated: {output_file}")
        
    except Exception as e:
        logger.error(f"Excel generation failed: {str(e)}")
        return
    
    # Final report
    logger.info(f"Total stocks processed: {len(all_stocks)}")
    logger.info(f"Stocks with RSI < 50: {len(low_rsi_stocks)}")
    logger.info(f"Failed symbols: {len(failed_symbols)}")
    
    # Save failed symbols to file
    if failed_symbols:
        with open(f"NEPSE_Reports/failed_symbols_{today}.txt", "w") as f:
            f.write("\n".join(failed_symbols))

if __name__ == "__main__":
    main()
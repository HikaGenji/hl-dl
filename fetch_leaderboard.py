
import requests
import pandas as pd
import json
from datetime import datetime
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_hyperliquid_leaderboard():
    """
    Fetch leaderboard data from Hyperliquid API and process it into expanded columns
    """
    url = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"

    try:
        logger.info(f"Trying stats-data endpoint: {url}")
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            logger.info("Successfully fetched data from stats-data endpoint")
            data = response.json()
            logger.info(f"Response contains {type(data).__name__} entries")

            if 'leaderboardRows' in data:
                return data['leaderboardRows']
            else:
                logger.error(f"Unexpected response structure: {list(data.keys())}")
                return None

        else:
            logger.error(f"API request failed with status {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return None

def expand_leaderboard_data(leaderboard_rows):
    """
    Expand nested windowPerformances data into separate columns
    """
    logger.info("Expanding nested windowPerformances data...")

    expanded_data = []

    for row in leaderboard_rows:
        # Start with basic information
        expanded_row = {
            'timestamp': datetime.utcnow().isoformat(),
            'ethAddress': row.get('ethAddress', ''),
            'accountValue': float(row.get('accountValue', 0)) if row.get('accountValue') else 0,
            'displayName': row.get('displayName', ''),
            'prize': int(row.get('prize', 0)) if row.get('prize') else 0
        }

        # Process windowPerformances array
        window_performances = row.get('windowPerformances', [])

        for performance_data in window_performances:
            if len(performance_data) == 2:
                time_period = performance_data[0]  # e.g., "day", "week", "month", "allTime"
                metrics = performance_data[1]      # Dictionary with pnl, roi, vlm

                # Extract metrics and create columns
                pnl = float(metrics.get('pnl', 0)) if metrics.get('pnl') else 0
                roi = float(metrics.get('roi', 0)) if metrics.get('roi') else 0
                volume = float(metrics.get('vlm', 0)) if metrics.get('vlm') else 0  # vlm = volume

                # Add to expanded row
                expanded_row[f'{time_period}_pnl'] = pnl
                expanded_row[f'{time_period}_roi'] = roi
                expanded_row[f'{time_period}_volume'] = volume

        expanded_data.append(expanded_row)

    return pd.DataFrame(expanded_data)

def save_to_csv(df, output_dir='data'):
    """
    Save dataframe to CSV with current date
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename with current date
    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    filename = f"leaderboard_{current_date}.parquet"
    filepath = os.path.join(output_dir, filename)

    # Save to parquet
    df.to_parquet(filepath, index=False)

    logger.info(f"Saved {len(df)} rows to {filepath}")
    logger.info(f"Columns: {list(df.columns)}")

    # Get performance columns (those ending with _pnl, _roi, _volume)
    perf_columns = [col for col in df.columns if col.endswith(('_pnl', '_roi', '_volume'))]
    logger.info(f"Performance columns ({len(perf_columns)}): {perf_columns}")

    # Preview data
    logger.info("Data preview:")
    logger.info("\nFirst 3 rows of expanded data:")
    print(df.head(3))
    logger.info(f"\nData shape: {df.shape}")
    logger.info(f"Saved to: {filepath}")

    return filepath

def main():
    """
    Main function to fetch and process leaderboard data
    """
    # Fetch raw data
    leaderboard_rows = fetch_hyperliquid_leaderboard()

    if leaderboard_rows is None:
        logger.error("Failed to fetch leaderboard data")
        return

    # Expand the data
    df = expand_leaderboard_data(leaderboard_rows)

    if df.empty:
        logger.error("No data to save")
        return

    # Save to CSV
    filepath = save_to_csv(df)

    return filepath

if __name__ == "__main__":
    main()

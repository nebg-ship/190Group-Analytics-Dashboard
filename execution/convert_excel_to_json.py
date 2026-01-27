import os
import re
import glob
import datetime
import json
import pandas as pd
import traceback

SOURCE_DIR = 'amazon_economics'
OUTPUT_FILE = 'temp_ingest/weekly_data.jsonl'

def normalize_column_name(name):
    if not name or pd.isna(name):
        return ""
    clean = re.sub(r'[^a-zA-Z0-9]', '_', str(name))
    clean = re.sub(r'_+', '_', clean).strip('_')
    return clean.lower()

def extract_dates_from_filename(filename):
    basename = os.path.basename(filename)
    match = re.search(r'(\d{1,2}-\d{1,2}-\d{2}) to (\d{1,2}-\d{1,2}-\d{2})', basename)
    if match:
        start_str, end_str = match.groups()
        return start_str, end_str
    return None, None

def process_file(file_path):
    print(f"Processing {file_path}...")
    start_date, end_date = extract_dates_from_filename(file_path)
    if not start_date:
        return []

    try:
        # Read with multi-level header
        df = pd.read_excel(file_path, header=[0, 1], engine='openpyxl')
        
        # Flatten headers
        new_cols = []
        last_cat = "info"
        for col in df.columns:
            cat = col[0]
            metric = col[1]
            
            if not str(cat).startswith('Unnamed'):
                last_cat = str(cat)
            
            if str(metric).startswith('Unnamed'):
                col_name = last_cat
            else:
                col_name = f"{last_cat}_{metric}"
            
            new_cols.append(normalize_column_name(col_name))
            
        df.columns = new_cols
        
        # Metadata
        df['report_start_date'] = pd.to_datetime(start_date, format='%m-%d-%y').strftime('%Y-%m-%d')
        df['report_end_date'] = pd.to_datetime(end_date, format='%m-%d-%y').strftime('%Y-%m-%d')
        df['source_file'] = os.path.basename(file_path)
        
        # Convert to list of dicts
        # Replace NaN with None. astype(object) is needed to allow None in numeric columns
        return df.astype(object).where(pd.notnull(df), None).to_dict(orient='records')

    except Exception as e:
        print(f"Error {file_path}:")
        traceback.print_exc()
        return []

def main():
    if not os.path.exists('temp_ingest'):
        os.makedirs('temp_ingest')
        
    files = glob.glob(os.path.join(SOURCE_DIR, "*.xlsx"))
    all_data = []
    
    for f in files:
        file_data = process_file(f)
        all_data.extend(file_data)
        
    with open(OUTPUT_FILE, 'w') as f:
        for entry in all_data:
            # Handle datetime objects that might still be in the dict if any
            json.dump(entry, f, default=str)
            f.write('\n')
            
    print(f"Written {len(all_data)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

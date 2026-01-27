import os
import re
import glob
import datetime
import csv
from google.cloud import bigquery
from dotenv import load_dotenv
import openpyxl

# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
DATASET_ID = 'amazon_weekly_economics'
SOURCE_DIR = 'amazon_economics'

def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

def create_dataset_if_not_exists(client, dataset_id):
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_ref} already exists.")
    except Exception:
        print(f"Creating dataset {dataset_ref}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Dataset {dataset_ref} created.")

def normalize_column_name(name):
    if not name:
        return ""
    # Replace non-alphanumeric characters with underscore
    clean = re.sub(r'[^a-zA-Z0-9]', '_', str(name))
    # Remove leading/trailing underscores and multiple underscores
    clean = re.sub(r'_+', '_', clean).strip('_')
    return clean.lower()

def extract_dates_from_filename(filename):
    basename = os.path.basename(filename)
    # Try format: 1-15-26 to 1-21-26 (with dashes)
    match = re.search(r'(\d{1,2}-\d{1,2}-\d{2}) to (\d{1,2}-\d{1,2}-\d{2})', basename)
    if match:
        start_str, end_str = match.groups()
        return start_str, end_str
    # Try format: 1_15_25_to_1_21_25 (with underscores)
    match = re.search(r'(\d{1,2}_\d{1,2}_\d{2})_to_(\d{1,2}_\d{1,2}_\d{2})', basename)
    if match:
        start_str, end_str = match.groups()
        # Convert underscores to dashes for parsing
        start_str = start_str.replace('_', '-')
        end_str = end_str.replace('_', '-')
        return start_str, end_str
    return None, None

def parse_date(date_str):
    try:
        return datetime.datetime.strptime(date_str, '%m-%d-%y').date().isoformat()
    except ValueError:
        return None

def process_csv_file(file_path):
    print(f"Processing {file_path}...")
    
    start_date, end_date = extract_dates_from_filename(file_path)
    if not start_date or not end_date:
        print(f"Could not extract dates from {file_path}, skipping.")
        return None

    report_start = parse_date(start_date)
    report_end = parse_date(end_date)
    file_name = os.path.basename(file_path)

    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            
            # Read header rows (1 and 2)
            row1 = next(reader)  # Categories
            row2 = next(reader)  # Metrics
            
            # Construct flat column names
            headers = []
            last_category = "Info"
            
            for i in range(len(row1)):
                cat = row1[i]
                metric = row2[i] if i < len(row2) else None
                
                if cat and cat.strip():
                    last_category = str(cat)
                
                if not metric or not metric.strip():
                    if cat and cat.strip():
                        col_name = str(cat)
                    else:
                        col_name = f"col_{i}"
                else:
                    col_name = f"{last_category}_{metric}"
                
                # Normalize and ensure non-empty
                normalized = normalize_column_name(col_name)
                if not normalized:
                    normalized = f"col_{i}"
                
                headers.append(normalized)
            
            print(f"Found {len(headers)} columns.")
            
            data_rows = []
            for row in reader:
                # Skip empty rows
                if all(not cell or not str(cell).strip() for cell in row):
                    continue
                    
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(headers):
                        if not value or not str(value).strip():
                            row_dict[headers[i]] = None
                        else:
                            # Try to convert to float (not int) to match Excel behavior
                            try:
                                row_dict[headers[i]] = float(value)
                            except (ValueError, TypeError):
                                row_dict[headers[i]] = str(value)
                
                # Add metadata
                row_dict['report_start_date'] = report_start
                row_dict['report_end_date'] = report_end
                row_dict['source_file'] = file_name
                
                data_rows.append(row_dict)
                
            return data_rows

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def process_file(file_path):
    print(f"Processing {file_path}...")
    
    start_date, end_date = extract_dates_from_filename(file_path)
    if not start_date or not end_date:
        print(f"Could not extract dates from {file_path}, skipping.")
        return None

    report_start = parse_date(start_date)
    report_end = parse_date(end_date)
    file_name = os.path.basename(file_path)

    try:
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        ws = wb.active
        
        rows = ws.iter_rows(values_only=True)
        
        # Read header rows (1 and 2)
        row1 = next(rows) # Categories
        row2 = next(rows) # Metrics
        
        # Construct flat column names
        headers = []
        last_category = "Info"
        
        for i in range(len(row1)):
            cat = row1[i]
            metric = row2[i]
            
            if cat is not None:
                last_category = str(cat)
            
            if metric is None:
                if row1[i] is not None:
                     col_name = str(row1[i])
                else:
                    col_name = f"col_{i}"
            else:
                col_name = f"{last_category}_{metric}"
            
            # Normalize and ensure non-empty
            normalized = normalize_column_name(col_name)
            if not normalized:
                normalized = f"col_{i}"
            
            headers.append(normalized)
            
        print(f"Found {len(headers)} columns.")
        
        data_rows = []
        for row in rows:
            # Skip empty rows
            if all(cell is None for cell in row):
                continue
                
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(headers):
                    # Convert to string to ensure consistency (simulating pandas object->str)
                    # or keep as proper types if BigQuery can handle JSON types.
                    # Safest is to keep basic types or stringify.
                    # bigquery.load_table_from_json handles basic types.
                    if value is None:
                        row_dict[headers[i]] = None
                    elif isinstance(value, datetime.datetime):
                        row_dict[headers[i]] = value.isoformat()
                    elif isinstance(value, datetime.date):
                        row_dict[headers[i]] = value.isoformat()
                    else:
                        row_dict[headers[i]] = value
            
            # Add metadata
            row_dict['report_start_date'] = report_start
            row_dict['report_end_date'] = report_end
            row_dict['source_file'] = file_name
            
            data_rows.append(row_dict)
            
        return data_rows

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def main():
    client = get_bigquery_client()
    create_dataset_if_not_exists(client, DATASET_ID)
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.weekly_sku_economics"
    
    all_data = []
    xlsx_files = glob.glob(os.path.join(SOURCE_DIR, "**/*.xlsx"), recursive=True)
    csv_files = glob.glob(os.path.join(SOURCE_DIR, "**/*.csv"), recursive=True)
    
    files = xlsx_files + csv_files
    
    if not files:
        print(f"No .xlsx or .csv files found in {SOURCE_DIR}")
        return

    for f in files:
        if f.endswith('.csv'):
            file_data = process_csv_file(f)
        else:
            file_data = process_file(f)
        if file_data:
            all_data.extend(file_data)
    
    if not all_data:
        print("No data extracted.")
        return
        
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    print(f"Loading {len(all_data)} rows to {table_id}...")
    job = client.load_table_from_json(all_data, table_id, job_config=job_config)
    job.result()
    
    print(f"Loaded {job.output_rows} rows to {table_id}.")

if __name__ == "__main__":
    main()

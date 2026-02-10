"""
Prepare the master inventory CSV for Convex import.
Renames columns to match the Convex schema (underscores instead of spaces),
converts numeric fields, and outputs a clean CSV.
"""
import csv
import json
import re
import os
import sys

# Column mapping: CSV header -> Convex field name
COLUMN_MAP = {
    "Active Status": "Active_Status",
    "Type": "Type",
    "Sku": "Sku",
    "Description": "Description",
    "Sales Tax Code": "Sales_Tax_Code",
    "Account": "Account",
    "COGS Account": "COGS_Account",
    "Asset Account": "Asset_Account",
    "Accumulated Depreciation": "Accumulated_Depreciation",
    "Purchase Description": "Purchase_Description",
    "Quantity On Hand (2025)": "Quantity_On_Hand_2025",
    "U/M": "U_M",
    "U/M Set": "U_M_Set",
    "Cost": "Cost",
    "Preferred Vendor": "Preferred_Vendor",
    "Tax Agency": "Tax_Agency",
    "Price": "Price",
    "Reorder Pt (Min)": "Reorder_Pt_Min",
    "MPN": "MPN",
    "Category": "Category",
}

# Fields that should be numeric (number type in Convex)
NUMERIC_FIELDS = {"Accumulated_Depreciation", "Quantity_On_Hand_2025", "Cost", "Price", "Reorder_Pt_Min"}

def safe_float(value, default=0.0):
    """Convert a value to float, returning default if empty or invalid."""
    if not value or not value.strip():
        return default
    try:
        # Remove any currency symbols, commas
        cleaned = re.sub(r'[,$]', '', value.strip())
        return float(cleaned)
    except (ValueError, TypeError):
        return default

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_path = os.path.join(project_root, "amazon_economics", "Master_Updated_web_accounts_v14_1 (1).csv")
    output_path = os.path.join(project_root, ".tmp", "inventory_parts_clean.csv")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    rows_written = 0
    rows_skipped = 0
    
    with open(input_path, 'r', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        
        # Build output fieldnames
        out_fields = [COLUMN_MAP.get(h, h) for h in reader.fieldnames if h in COLUMN_MAP]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=out_fields)
            writer.writeheader()
            
            for row in reader:
                # Skip rows without a SKU
                sku = (row.get("Sku") or "").strip()
                if not sku:
                    rows_skipped += 1
                    continue
                
                out_row = {}
                for csv_col, convex_col in COLUMN_MAP.items():
                    val = row.get(csv_col, "")
                    if convex_col in NUMERIC_FIELDS:
                        out_row[convex_col] = safe_float(val)
                    else:
                        out_row[convex_col] = (val or "").strip()
                
                writer.writerow(out_row)
                rows_written += 1
    
    print(f"Done! Wrote {rows_written} rows to {output_path} (skipped {rows_skipped})")
    return output_path

if __name__ == "__main__":
    main()

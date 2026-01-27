import openpyxl
import os

file_path = 'amazon_economics/amz economics 1-1-26 to 1-7-26.xlsx'
output_file = 'inspection_result.txt'

try:
    wb = openpyxl.load_workbook(file_path, read_only=True)
    ws = wb.active
    rows = ws.iter_rows(min_row=1, max_row=3, values_only=True)
    
    with open(output_file, 'w') as f:
        row1 = next(rows)
        row2 = next(rows)
        row3 = next(rows)
        f.write(f"ROW 1: {list(row1)}\n")
        f.write(f"ROW 2: {list(row2)}\n")
        f.write(f"ROW 3: {list(row3)}\n")
        
    print(f"Written inspection to {output_file}")
except Exception as e:
    print(f"Error: {e}")

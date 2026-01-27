import pandas as pd
import os

file_path = 'amazon_economics/amz economics 1-1-26 to 1-7-26.xlsx'
if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
    exit(1)

try:
    df = pd.read_excel(file_path, nrows=5)
    print("COLUMNS:", df.columns.tolist())
    print("TYPES:", df.dtypes)
except Exception as e:
    print(f"Error reading file: {e}")

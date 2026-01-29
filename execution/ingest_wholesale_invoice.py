"""
Wholesale Invoice Ingestion Script
Parses Markdown invoice files and loads them into BigQuery
"""
import os
import re
import shutil
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'bonsai-outlet')
DATASET_ID = 'wholesale'
INVOICE_DIR = Path(__file__).parent.parent / 'invoices_to_ingest' / 'wholesale'
PROCESSED_DIR = INVOICE_DIR / 'processed'

def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

def parse_invoice(file_path: Path) -> dict:
    """Parse a Markdown invoice file and extract order data."""
    content = file_path.read_text(encoding='utf-8')
    
    # 1. Extract Order ID
    order_id = None
    order_match = re.search(r'(?:order #|Order: #)(\d+)', content, re.IGNORECASE)
    if order_match:
        order_id = order_match.group(1)
        
    # 2. Extract Order Date
    order_date = None
    date_match = re.search(r'Order Date:.*?(\d+)(?:st|nd|rd|th)?\s+(\w+)\s+(\d{4})', content, re.IGNORECASE)
    if date_match:
        day, month, year = date_match.groups()
        for fmt in ["%d %B %Y", "%d %b %Y"]:
            try:
                order_date = datetime.strptime(f"{day} {month} {year}", fmt).date()
                break
            except ValueError:
                pass

    # 3. Extract Customer Name (Look for Billing Details block)
    customer_name = ""
    customer_email = ""
    customer_phone = ""
    billing_address = ""
    
    bill_block_match = re.search(r'##+ Billing Details\s*\n(.*?)(?=\n##+|\n---|\n\*|\n\*\*Order)', content, re.DOTALL | re.IGNORECASE)
    if bill_block_match:
        block = bill_block_match.group(1).strip()
        # Look for labels
        name_match = re.search(r'(?:\*\s*)?\*\*Name:\*\*\s*(.+)', block, re.IGNORECASE)
        company_match = re.search(r'(?:\*\s*)?\*\*Company:\*\*\s*(.+)', block, re.IGNORECASE)
        email_match = re.search(r'(?:\*\s*)?\*\*Email:\*\*\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', block, re.IGNORECASE)
        phone_match = re.search(r'(?:\*\s*)?\*\*Phone:\*\*\s*([\d\-\(\)\s\+]+)', block, re.IGNORECASE)
        
        contact_name = name_match.group(1).strip() if name_match else ""
        company_name = company_match.group(1).strip() if company_match else ""
        
        if company_name:
            customer_name = company_name
            if contact_name:
                customer_name = f"{company_name} ({contact_name})"
        elif contact_name:
            customer_name = contact_name
        else:
            # Fallback for multi-line format without labels:
            # First line is usually **Name**, second line is Company
            lines = [l.strip() for l in block.split('\n') if l.strip()]
            if lines:
                name_match_fall = re.search(r'\*\*(.+?)\*\*', lines[0])
                if name_match_fall:
                    contact_name = name_match_fall.group(1).strip()
                    if len(lines) > 1 and not any(k in lines[1] for k in ['**', '*', 'Email:', 'Phone:']):
                        company_name = lines[1].strip()
                        customer_name = f"{company_name} ({contact_name})"
                    else:
                        customer_name = contact_name
                else:
                    customer_name = lines[0].strip().strip('*').strip()
            
        if email_match: customer_email = email_match.group(1).strip()
        if phone_match: customer_phone = phone_match.group(1).strip()
        
        # Address is usually the lines that aren't phone/email/labels
        addr_lines = []
        for line in block.split('\n'):
            line = line.strip().strip('*').strip()
            if not line or 'Phone:' in line or 'Email:' in line or 'Name:' in line or 'Order:' in line or 'Payment' in line:
                continue
            addr_lines.append(line)
        billing_address = ', '.join(addr_lines)

    # 4. Extract Totals (handle lists, tables, bolding)
    def clean_amount(val):
        if not val: return 0.0
        return float(re.sub(r'[^\d.]', '', val.replace(',', '')))

    subtotal = clean_amount(re.search(r'Subtotal.*?\$?([\d,\.]+)', content, re.IGNORECASE).group(1) if re.search(r'Subtotal.*?\$?([\d,\.]+)', content, re.IGNORECASE) else "0")
    shipping = clean_amount(re.search(r'Shipping.*?\$?([\d,\.]+)', content, re.IGNORECASE).group(1) if re.search(r'Shipping.*?\$?([\d,\.]+)', content, re.IGNORECASE) else "0")
    tax = clean_amount(re.search(r'Tax.*?\$?([\d,\.]+)', content, re.IGNORECASE).group(1) if re.search(r'Tax.*?\$?([\d,\.]+)', content, re.IGNORECASE) else "0")
    grand_total = clean_amount(re.search(r'Grand total.*?\$?([\d,\.]+)', content, re.IGNORECASE).group(1) if re.search(r'Grand total.*?\$?([\d,\.]+)', content, re.IGNORECASE) else "0")

    # 5. Extract Shipping Method
    ship_method_match = re.search(r'Shipping Method:\s*(.+)', content, re.IGNORECASE)
    shipping_method = ship_method_match.group(1).strip().strip('*').strip() if ship_method_match else ""
    
    # 6. Extract Payment Method
    pay_match = re.search(r'Payment Method:\s*(.+?)(?:\s*\(\$[\d,\.]+\))?$', content, re.MULTILINE | re.IGNORECASE)
    payment_method = pay_match.group(1).strip().strip('*').strip() if pay_match else ""

    # 7. Extract Line Items
    line_items = []
    table_pattern = r'\|\s*(\d+)\s*\|\s*([A-Za-z0-9\-]+)\s*\|\s*(.+?)\s*\|\s*(\*\*)?\$?([\d,\.]+)\s*(?:USD)?\s*(\*\*)?\|\s*(\*\*)?\$?([\d,\.]+)\s*(?:USD)?\s*(\*\*)?\|'
    for i, match in enumerate(re.finditer(table_pattern, content), 1):
        qty, sku, product, _, unit_price, _, _, line_total, _ = match.groups()
        line_items.append({
            'order_id': order_id,
            'line_number': i,
            'sku': sku.strip(),
            'product_name': product.strip(),
            'quantity': int(qty),
            'unit_price': float(unit_price.replace(',', '')),
            'line_total': float(line_total.replace(',', ''))
        })
    
    header = {
        'order_id': order_id,
        'order_date': order_date,
        'customer_name': customer_name,
        'customer_email': customer_email,
        'customer_phone': customer_phone,
        'billing_address': billing_address,
        'shipping_address': billing_address,
        'shipping_method': shipping_method,
        'subtotal': subtotal,
        'shipping': shipping,
        'tax': tax,
        'grand_total': grand_total,
        'payment_method': payment_method
    }
    
    return {'header': header, 'line_items': line_items}

def load_to_bigquery(data: dict):
    """Load parsed invoice data into BigQuery."""
    client = get_bigquery_client()
    
    # Insert header
    header_table = f"{PROJECT_ID}.{DATASET_ID}.order_header"
    header_row = data['header'].copy()
    header_row['order_date'] = header_row['order_date'].isoformat() if header_row['order_date'] else None
    
    errors = client.insert_rows_json(header_table, [header_row])
    if errors:
        print(f"Error inserting header: {errors}")
        return False
    
    # Insert line items
    if data['line_items']:
        items_table = f"{PROJECT_ID}.{DATASET_ID}.order_line_items"
        errors = client.insert_rows_json(items_table, data['line_items'])
        if errors:
            print(f"Error inserting line items: {errors}")
            return False
    
    return True

def process_invoices():
    """Process all pending invoice files."""
    PROCESSED_DIR.mkdir(exist_ok=True)
    
    processed_count = 0
    for file_path in INVOICE_DIR.glob('*.md'):
        if file_path.name == 'README.md':
            continue
            
        print(f"Processing: {file_path.name}")
        
        try:
            data = parse_invoice(file_path)
            
            if not data['header']['order_id']:
                print(f"  Skipping - could not extract order ID")
                continue
            
            print(f"  Order ID: {data['header']['order_id']}")
            print(f"  Items: {len(data['line_items'])}")
            print(f"  Total: ${data['header']['grand_total']:.2f}")
            
            if load_to_bigquery(data):
                # Move to processed folder
                dest = PROCESSED_DIR / file_path.name
                shutil.move(str(file_path), str(dest))
                print(f"  Loaded to BigQuery and moved to processed/")
                processed_count += 1
            else:
                print(f"  Failed to load to BigQuery")
                
        except Exception as e:
            print(f"  Error: {e}")
    
    print(f"\nProcessed {processed_count} invoice(s)")

if __name__ == '__main__':
    process_invoices()

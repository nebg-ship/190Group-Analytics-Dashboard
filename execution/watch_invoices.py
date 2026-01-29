"""
Invoice Folder Watcher
Monitors the invoices_to_ingest folder and automatically processes new invoices.
"""
import time
import sys
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Add execution folder to path for imports
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from ingest_wholesale_invoice import parse_invoice, load_to_bigquery, PROCESSED_DIR

WATCH_DIR = SCRIPT_DIR.parent / 'invoices_to_ingest' / 'wholesale'

class InvoiceHandler(FileSystemEventHandler):
    """Handle new invoice files."""
    
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        # Only process markdown files
        if file_path.suffix.lower() != '.md':
            return
        
        # Skip README files
        if file_path.name.lower() == 'readme.md':
            return
        
        # Wait a moment for file to finish writing
        time.sleep(1)
        
        print(f"\n[NEW FILE] {file_path.name}")
        
        try:
            data = parse_invoice(file_path)
            
            if not data['header']['order_id']:
                print(f"  Skipping - could not extract order ID")
                return
            
            print(f"  Order ID: {data['header']['order_id']}")
            print(f"  Items: {len(data['line_items'])}")
            print(f"  Total: ${data['header']['grand_total']:.2f}")
            
            PROCESSED_DIR.mkdir(exist_ok=True)
            
            if load_to_bigquery(data):
                # Move to processed folder
                import shutil
                dest = PROCESSED_DIR / file_path.name
                shutil.move(str(file_path), str(dest))
                print(f"  SUCCESS: Loaded to BigQuery and moved to processed/")
            else:
                print(f"  FAILED: Could not load to BigQuery")
                
        except Exception as e:
            print(f"  ERROR: {e}")

def main():
    print("=" * 60)
    print("WHOLESALE INVOICE WATCHER")
    print("=" * 60)
    print(f"Watching: {WATCH_DIR}")
    print("Drop invoice files (.md) into this folder for auto-ingestion.")
    print("Press Ctrl+C to stop.")
    print("=" * 60)
    
    # Ensure watch directory exists
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    
    event_handler = InvoiceHandler()
    observer = Observer()
    observer.schedule(event_handler, str(WATCH_DIR), recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping watcher...")
        observer.stop()
    
    observer.join()
    print("Watcher stopped.")

if __name__ == '__main__':
    main()

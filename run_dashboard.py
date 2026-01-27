"""
CEO Dashboard Runner
Starts the Flask API server and opens the dashboard in the browser
"""
import os
import sys
import webbrowser
import time
from pathlib import Path

def main():
    # Get the project root directory
    project_root = Path(__file__).parent
    
    print("=" * 60)
    print("190 Group Analytics - CEO Dashboard")
    print("=" * 60)
    print()
    print("Starting Flask API server...")
    print("Dashboard will open automatically in your browser.")
    print()
    print("Press Ctrl+C to stop the server.")
    print("=" * 60)
    print()
    
    # Open dashboard in browser after a short delay
    time.sleep(2)
    dashboard_url = "http://localhost:5000"
    webbrowser.open(dashboard_url)
    
    # Start Flask server
    sys.path.insert(0, str(project_root / 'api'))
    from dashboard_data import app
    app.run(debug=True, port=5000, use_reloader=False)

if __name__ == '__main__':
    main()

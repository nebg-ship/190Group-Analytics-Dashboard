import os
from dotenv import load_dotenv

def validate():
    load_dotenv()
    keys = [
        "SP_API_REFRESH_TOKEN",
        "SP_API_CLIENT_ID",
        "SP_API_CLIENT_SECRET",
        "SP_API_AWS_ACCESS_KEY",
        "SP_API_AWS_SECRET_KEY"
    ]
    
    for k in keys:
        v = os.getenv(k)
        if not v:
            print(f"{k}: MISSING")
            continue
            
        print(f"{k}:")
        print(f"  Length: {len(v)}")
        print(f"  Starts with: {v[:5]}...")
        print(f"  Ends with: ...{v[-5:]}")
        print(f"  Contains whitespace: {'Yes' if any(c.isspace() for c in v) else 'No'}")
        # Check if it contains the literal name of the variable (indicating replacement failed)
        if k in v:
            print(f"  WARNING: Value contains its own key name. Replacement likely failed.")

if __name__ == "__main__":
    validate()

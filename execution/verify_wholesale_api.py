import requests
import json

def verify_api():
    output = []
    try:
        response = requests.get('http://localhost:5000/api/dashboard')
        if response.status_code == 200:
            data = response.json()
            if 'wholesale_customers' in data:
                customers = data['wholesale_customers']
                output.append(f"SUCCESS: 'wholesale_customers' field found with {len(customers)} records.")
                
                target = next((c for c in customers if 'Stone Lantern' in c.get('company_name', '')), None)
                if target:
                    output.append("Found target customer:")
                    output.append(json.dumps(target, indent=2))
                else:
                    output.append("FAILURE: California Carnivores not found in results.")
                    output.append("Top 5 records:")
                    output.append(json.dumps(customers[:5], indent=2))
            else:
                output.append("FAILURE: 'wholesale_customers' field missing from response.")
        else:
            output.append(f"FAILURE: API returned status code {response.status_code}")
            output.append(response.text)
    except Exception as e:
        output.append(f"FAILURE: Error calling API: {e}")

    with open('verification_result.txt', 'w') as f:
        f.write('\n'.join(output))

if __name__ == "__main__":
    verify_api()

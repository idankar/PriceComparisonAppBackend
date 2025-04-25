# api_key_check.py
import requests
import argparse

def check_api_key(api_key):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    # Make a simple API request to check key validity
    url = "https://api.openai.com/v1/models"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        print("API key is valid! ✅")
        models = response.json()
        print(f"Available models: {len(models['data'])}")
        return True
    elif response.status_code == 401:
        print("API key is invalid! ❌")
        print(f"Error: {response.json().get('error', {}).get('message', 'Unknown error')}")
        return False
    elif response.status_code == 429:
        print("API key is valid but rate limited! ⚠️")
        print(f"Error: {response.json().get('error', {}).get('message', 'Unknown error')}")
        return True
    else:
        print(f"Unexpected status code: {response.status_code}")
        print(f"Response: {response.text}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check OpenAI API key validity")
    parser.add_argument("--api-key", required=True, help="OpenAI API key to check")
    
    args = parser.parse_args()
    
    check_api_key(args.api_key)
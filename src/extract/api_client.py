import os
import requests
from dotenv import load_dotenv
import yaml

class APIClient:
    def __init__(self, settings_path="config/settings.yaml"):
        # Load API key
        load_dotenv()
        self.api_key = os.getenv("API_KEY")
        if not self.api_key:
            raise ValueError("API_KEY not found. Make sure it's in your .env file.")

        # Load settings.yaml
        with open(settings_path, "r") as f:
            settings = yaml.safe_load(f)

        self.base_url = settings["api_base_url"]
        self.seasons = settings["seasons"]

        # Default headers for API-Football
        self.headers = {
            "x-apisports-key": self.api_key
        }

    def get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)

        if response.status_code != 200:
            raise Exception(f"HTTP error {response.status_code}: {response.text}")

        data = response.json()

        # API-level errors
        if data.get("errors"):
            print(f"API error: {data['errors']}")
            return None

        return data
from src.extract.api_client import APIClient

client = APIClient()

# Test: get La Liga leagues info
data = client.get("leagues", params={"id": 140})
print(data)
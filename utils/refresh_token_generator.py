import requests
import webbrowser
from urllib.parse import urlencode
import os

APP_KEY = os.getenv("DROPBOX_APP_KEY")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
REDIRECT_URI = "http://localhost:8080"

# Step 1: Direct user to authorization URL
params = {
    "client_id": APP_KEY,
    "response_type": "code",
    "token_access_type": "offline",
    "redirect_uri": REDIRECT_URI,
}
auth_url = f"https://www.dropbox.com/oauth2/authorize?{urlencode(params)}"
print("Open this URL in your browser to authorize:")
print(auth_url)
webbrowser.open(auth_url)

# Step 2: After authorizing, Dropbox redirects to localhost with a ?code= param
auth_code = input("Paste the 'code' parameter from the URL here: ")

# Step 3: Exchange code for refresh token
token_url = "https://api.dropbox.com/oauth2/token"
data = {
    "code": auth_code,
    "grant_type": "authorization_code",
    "redirect_uri": REDIRECT_URI,
}
response = requests.post(token_url, data=data, auth=(APP_KEY, APP_SECRET))
print("Response:", response.json())

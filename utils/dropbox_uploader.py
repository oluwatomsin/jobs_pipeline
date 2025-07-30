import os
import requests
import dropbox
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")


class DropboxUploader:
    def __init__(self, dropbox_folder):
        """
        Initialize the Dropbox uploader using refresh-token-based OAuth.
        :param dropbox_folder: Folder path in Dropbox where files will be uploaded.
        """
        self.dropbox_folder = dropbox_folder
        self.access_token = self._get_access_token()  # ✅ now private inside class
        self.dbx = dropbox.Dropbox(self.access_token)

    def _get_access_token(self):
        """🔒 Private: Fetch a new access token using the refresh token."""
        url = "https://api.dropboxapi.com/oauth2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN,
            "client_id": APP_KEY,
            "client_secret": APP_SECRET,
        }
        response = requests.post(url, data=data)
        response.raise_for_status()
        token = response.json()["access_token"]
        print("✅ Access token refreshed.")
        return token

    def _generate_dropbox_path(self, local_file_path):
        """Generate a Dropbox file path with today's date appended."""
        today = datetime.now().strftime("%Y-%m-%d")
        base, ext = os.path.splitext(os.path.basename(local_file_path))
        dropbox_filename = f"{base}_{today}{ext}"
        return f"{self.dropbox_folder}/{dropbox_filename}"

    def _upload_file(self, local_file_path, dropbox_path):
        """Upload the file to Dropbox."""
        with open(local_file_path, "rb") as f:
            self.dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        print(f"✅ Uploaded to Dropbox: {dropbox_path}")

    def _get_shareable_link(self, dropbox_path):
        """Generate or retrieve a shareable download link."""
        try:
            link = self.dbx.sharing_create_shared_link_with_settings(dropbox_path).url
        except dropbox.exceptions.ApiError:
            links = self.dbx.sharing_list_shared_links(path=dropbox_path).links
            link = links[0].url if links else None

        if link:
            link = link.replace("?dl=0", "?dl=1")
            print(f"🔗 Download Link: {link}")
        else:
            print("⚠️ Could not generate share link.")
        return link

    def upload(self, local_file_path):
        """
        Upload a local file to Dropbox and return a shareable direct download link.
        """
        dropbox_path = self._generate_dropbox_path(local_file_path)
        self._upload_file(local_file_path, dropbox_path)
        return self._get_shareable_link(dropbox_path)
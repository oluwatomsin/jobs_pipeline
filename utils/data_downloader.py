import os
import io
import tempfile
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from datetime import datetime, timezone


class GoogleDriveDownloader:
    SCOPES = ['https://www.googleapis.com/auth/drive']

    def __init__(self, folder_name, service_account_file):
        self.folder_name = folder_name
        self.service_account_file = service_account_file
        self.service = self._authenticate_service_account()

    def _authenticate_service_account(self):
        """Authenticate using the service account JSON."""
        creds = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=self.SCOPES
        )
        return build('drive', 'v3', credentials=creds)

    def _get_folder_id_by_name(self):
        """Get a folder's ID by its name."""
        query = (
            f"name='{self.folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])

        if not folders:
            raise ValueError(f"❌ Folder '{self.folder_name}' not found or not shared with service account.")

        folder_id = folders[0]['id']  # take the first match
        print(f"📂 Found folder '{self.folder_name}' (ID: {folder_id})")
        return folder_id

    def _list_files_in_folder(self, folder_id):
        """List all files inside a specific folder by ID."""
        query = f"'{folder_id}' in parents and trashed=false"
        results = self.service.files().list(
            q=query,
            fields="files(id, name, createdTime, mimeType)"
        ).execute()
        return results.get('files', [])

    def _filter_files_created_today(self, files):
        """Return only files created today (UTC)."""
        today = datetime.now(timezone.utc).date()
        return [
            f for f in files
            if datetime.fromisoformat(f['createdTime'].replace('Z', '+00:00')).date() == today
        ]

    def _download_file_to_temp(self, file_id, file_name):
        """Download a Drive file to a temporary file and return its local path."""
        temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(temp_dir, file_name)

        request = self.service.files().get_media(fileId=file_id)
        with io.FileIO(local_path, 'wb') as fh:
            download = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = download.next_chunk()
                if status:
                    print(f"⬇️  Downloading {file_name}... {int(status.progress() * 100)}%")

        print(f"✅ Downloaded to temp: {local_path}")
        return local_path

    def runner(self) -> list:
        """Execute the process: authenticate, list today's files, and download them."""
        folder_id = self._get_folder_id_by_name()
        files = self._list_files_in_folder(folder_id)
        todays_files = self._filter_files_created_today(files)

        if not todays_files:
            print("⚠️ No files created today.")
            return []

        downloaded_files = []
        for f in todays_files:
            local_path = self._download_file_to_temp(f['id'], f['name'])
            downloaded_files.append(local_path)

        return downloaded_files
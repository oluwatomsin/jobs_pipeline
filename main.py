from utils.data_cleaner import DataPreprocessor
from utils.data_downloader import GoogleDriveDownloader
from utils.dropbox_uploader import DropboxUploader



# Download file for today from Google Drive.

def orchestrate():
    gdrive_folder_name = "Apify Uploads"
    config_file = "config/cleaner.yml"
    cleaned_file_name = "/tmp/cleaned_data.csv"
    my_dropbox_folder = "/ApifyCleaned"
    service_account_credentials = "credentials.json"


    # Downloading files that were created today from Google Drive.
    downloader = GoogleDriveDownloader(
            folder_name=gdrive_folder_name,
            service_account_file= service_account_credentials
        )
    downloaded_files = downloader.runner()
    print("Downloaded files:", downloaded_files)


    # Preprocessing file
    preprocessor = DataPreprocessor(config_file, downloaded_files)
    is_not_empty = preprocessor.runner(output_path=cleaned_file_name)

    # checking to see if there were no files to process
    if is_not_empty:
        # Upload the file to dropbox
        uploader = DropboxUploader(dropbox_folder=my_dropbox_folder)

        link = uploader.upload(cleaned_file_name)
        print("Final link:", link)
    else:
        print("No file to save")

if __name__ == '__main__':
    orchestrate()
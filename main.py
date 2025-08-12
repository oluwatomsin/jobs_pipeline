from utils.data_cleaner import DataPreprocessor
from utils.salesforce_data_dowloader import SalesforceReportDownloader, remove_existing_companies
from utils.data_downloader import GoogleDriveDownloader
from utils.dropbox_uploader import DropboxUploader
from dotenv import load_dotenv
from ai_filters.jd_qualifier import JobClassifier
import pandas as pd
import os
import asyncio
from ai_filters.web_search import CompanySizeFiller
from ai_filters.company_classifier import CompanyClassifier


load_dotenv()


username='oas@salariasales.com'
report_id = "00OPF000006VAVt2AO"

password=os.getenv("SF_password")
security_token=os.getenv("SF_security_token")



# Download the file for today from Google Drive.

def orchestrate():
    gdrive_folder_name = "Apify Uploads"
    config_file = "config/cleaner.yml"
    cleaned_file_name = "/tmp/cleaned_data.csv"
    my_dropbox_folder = "/ApifyCleaned"
    service_account_credentials = "credentials.json"
    salesforce_file_name = "/tmp/salesforce_report.csv"
    filtered_data_file_path = "/tmp/filtered_data.csv"
    jd_classified_path = "/tmp/classified_data.csv"
    filled_path = "/tmp/filled_data.csv"
    final_data = "/tmp/Completed.csv"


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

        # Remove prospected companies in the past 3 months
        # Step1: Downloading the sales force report
        downloader = SalesforceReportDownloader(
            username=username,
            password=password,
            security_token=security_token)
        downloader.authenticate()
        report_file_path = downloader.download_report(report_id=report_id, output_path=salesforce_file_name)
        print("File available at:", report_file_path)

        # Step2: Filtering out the data
        remove_existing_companies(cleaned_file=cleaned_file_name,
                                  salesforce_file=salesforce_file_name,
                                  output_file=filtered_data_file_path)

        # Job Classification
        df = pd.read_csv(filtered_data_file_path)
        classifier = JobClassifier(limit_rows=None)
        async def run_job_classification():
            processed_df = await classifier.process_dataframe(df)
            processed_df.to_csv(jd_classified_path, index=False)  # overwrite file

        asyncio.run(run_job_classification())


        # Company Data Enrichment
        filler = CompanySizeFiller()
        filler.fill_missing_sizes(file_path=jd_classified_path, output_path=filled_path)

        # Company classification
        async def main():
            company_classifier = CompanyClassifier()
            await company_classifier.process_dataset(
                input_csv_path=filled_path,
                output_csv_path=final_data)

        asyncio.run(main())

        # Upload the file to dropbox
        uploader = DropboxUploader(dropbox_folder=my_dropbox_folder)

        link = uploader.upload(final_data)
        print("Final link:", link)
    else:
        print("No file to save")

if __name__ == '__main__':
    orchestrate()